"""Schedule extraction tasks and clean up completed dependencies."""
import logging
import os
import signal
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import RLock
from typing import TYPE_CHECKING

from sqlalchemy.exc import SQLAlchemyError

from datakiln.extraction.models import ExtractGraph, ExtractMeta
from datakiln.metadata.graph import Unresolved

if TYPE_CHECKING:
    from datakiln.extraction.strategies import ExecutionStrategy

logger = logging.getLogger(__name__)


class ParallelExecutor:
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    """Orchestrates parallel execution with reference counting for cleanup."""

    def __init__(
        self,
        strategy: 'ExecutionStrategy',
        graph: 'ExtractGraph',
        max_workers: int = 4
    ):
        self.strategy = strategy
        self.graph = graph
        self.strategy.graph = graph
        self.max_workers = max_workers
        self.lock = RLock()

        # Tracking dependencies
        self.ref_counts = dict(graph.digraph.out_degree())

        # graphlib topological sorter for parallel orchestration
        self.ts = graph.topological_sorter
        self.live_tasks = set()

        # Register signal handlers for clean abort
        self._register_signal_handlers()

    def _register_signal_handlers(self):
        """Register signal handlers to guarantee cleanup on abort."""
        def _handler(signum, _frame):
            logger.error("Pipeline: Received signal %s, aborting.", signum)
            self.strategy.abort.set()
            self.strategy.cleanup()
            os._exit(1)

        # Only register if running in the main thread
        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGINT, _handler)
            signal.signal(signal.SIGTERM, _handler)

    def execute(
        self,
        data_dir: Path
    ):
        """Run tasks in parallel using TopologicalSorter interactive mode.

        :param data_dir: Directory of the data of the tasks to export.
        :type data_dir: Path
        """
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {}

            while self.ts.is_active():
                # Submit all ready tasks
                for node in self.ts.get_ready():
                    task = executor.submit(
                        self._run_task,
                        node,
                        data_dir
                    )
                    futures[task] = node

                if not futures:
                    # This shouldn't happen if is_active is true,
                    # unless there's a deadlock or we're waiting for futures.
                    if any(f.running() for f in futures):
                        pass
                    else:
                        break

                # Wait for at least one task to complete
                for future in as_completed(futures):
                    node = futures.pop(future)
                    try:
                        future.result()
                        with self.lock:
                            self.ts.done(node)
                            # Trigger cleanup and get new ready tasks
                            self._on_task_complete(node)
                        break  # Go back to the loop to submit new ready tasks
                    except (SQLAlchemyError, OSError) as e:
                        logger.error(
                            "Task '%s': Failed (%s).", node.full_name, e)
                        self.strategy.cleanup()
                        os._exit(1)

    def _run_task(
        self,
        node: ExtractMeta | Unresolved,
        data_dir: Path
    ):
        """Execute a task (table extraction or cohort initialization)."""
        # Start a managed session for this task
        with self.strategy.session():
            # Table extraction task
            if isinstance(node, ExtractMeta):
                export_to = data_dir if node in self.graph.export else None

                with self.lock:
                    self.live_tasks.add(node)

                self.strategy.execute(node, export_to=export_to)
            # Cohort node
            else:
                self.strategy.get_cohort(node.full_name)

    def _on_task_complete(self, node: ExtractMeta | Unresolved):
        """Perform cleanup for dependencies no longer needed."""
        to_drop_cohorts = []
        to_drop = []

        with self.lock:
            # Cohorts currently have no prerequisites in the graph
            for dep in self.graph.dependencies(node):
                # Decrement ref counts of dependencies used by THIS node
                self.ref_counts[dep] -= 1

                # If no other nodes depend on this dependency, drop it
                if self.ref_counts[dep] == 0:
                    if dep.full_name in self.strategy.cohorts:
                        to_drop_cohorts.append(dep.full_name)
                    elif dep in self.live_tasks:
                        to_drop.append(dep.temp_name)
                        self.live_tasks.remove(dep)

            # Check if THIS node itself is no longer needed by anyone else
            if self.ref_counts.get(node, 0) == 0:
                if isinstance(node, ExtractMeta):
                    to_drop.append(node.temp_name)
                    self.live_tasks.discard(node)
                elif node.full_name in self.strategy.cohorts:
                    to_drop_cohorts.append(node.full_name)

        # Execute drops outside of the lock
        for dep in to_drop_cohorts:
            self.strategy.drop_cohort(dep)
        for name in to_drop:
            self.strategy.drop(name)
