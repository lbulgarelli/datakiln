"""
Execution strategies and parallel orchestration for the extraction pipeline.
"""
import json
import logging
import hashlib
import threading
from itertools import chain
from pathlib import Path
from textwrap import dedent
from abc import ABC, abstractmethod
from contextlib import contextmanager
from functools import cached_property
from threading import RLock
from typing import List, Dict, Set, Optional, Iterator, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from sqlalchemy import (
    MetaData, Table, Column, select, literal, and_, text,
    values, column as sa_column, bindparam, String
)
from sqlalchemy.schema import DropTable
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import Select
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.selectable import Values

from data_utils.metadata.database import DatabaseMeta, Session
from data_utils.extraction.models import (
    UUID, ExtractMeta, ExtractGraph, FilterConfig
)
from data_utils.extraction.renderers import QueryDirector
from data_utils.extraction.exporters import MetadataExtractor, DataExporter


@compiles(Values, "oracle")
def compile_oracle_values(element, compiler, **kwargs):
    """Translate a Values clause to Oracle JSON_TABLE query."""
    columns = element.columns
    raw_data = element._data[0] if element._data else []  # pylint: disable=protected-access  # noqa: E501
    json_str = json.dumps(raw_data)

    bind_name = f"seed_list_{id(element)}"
    bind_param = bindparam(bind_name, value=json_str)
    bind_placeholder = compiler.process(bind_param, **kwargs)

    columns_spec = []
    for col_idx, col in enumerate(columns):
        if isinstance(col.type, String) and not col.type.length:
            col_type = "VARCHAR2(4000)"
        else:
            col_type = compiler.dialect.type_compiler.process(col.type)
        columns_spec.append(f"{col.name} {col_type} PATH '$[{col_idx}]'")

    return dedent(f"""
    (
      SELECT jt.*
      FROM JSON_TABLE({bind_placeholder}, '$[*]'
      COLUMNS ({', '.join(columns_spec)})) jt
    ) {compiler.preparer.quote(element.name)}
    """).strip()


class HashingReader:
    # pylint: disable=too-few-public-methods
    """File-like wrapper that calculates MD5 hash on the fly while reading."""

    def __init__(self, fp):
        self.fp = fp
        self.hasher = hashlib.md5()

    def read(self, size=-1):
        """Read data and update hasher."""
        chunk = self.fp.read(size)
        self.hasher.update(chunk)
        return chunk

    def seek(self, offset, whence=0):
        """Delegate seek to the underlying file pointer."""
        return self.fp.seek(offset, whence)

    def tell(self):
        """Delegate tell to the underlying file pointer."""
        return self.fp.tell()


# pylint: disable=too-many-instance-attributes
class ExecutionStrategy(ABC):
    """Abstract base class for extraction strategies."""

    def __init__(self, db_params: Dict, directory: Path):
        self.db_params = db_params
        self.directory = directory
        self.cohorts = {}
        self.created_tables = set()
        self.lock = threading.Lock()
        self._local = threading.local()
        self.csv_hashes = {}
        self.graph = None

    @cached_property
    def engine(self):
        """Returns the shared database engine."""
        with self.session(bind=False) as session:
            return session.get_bind()

    @contextmanager
    def session(self, bind: Optional[bool] = True) -> Iterator[Session]:
        """Returns a managed database session.

        If a session is already active for this thread, it is yielded.
        Otherwise, a new pooled session is created and registered as active.

        :param bind: Whether to use the shared engine. If False, a new
                     engine is created using Session.create().
        """
        if not bind:
            with Session.create(**self.db_params) as session:
                yield session
            return

        # Return thread-local session if active
        if hasattr(self._local, "session"):
            yield self._local.session
            return

        # Create a new managed session for this thread
        with Session(bind=self.engine, autoflush=True) as session:
            self._local.session = session
            try:
                yield session
            finally:
                if hasattr(self._local, "session"):
                    del self._local.session

    def build_queries(self, task: ExtractMeta) -> List[Select]:
        """Build the list of extraction SA Select statements.

        :param task: Extraction task metadata.
        :type task: ExtractMeta

        :return: List of SA Select statements.
        :rtype: list[Select]
        """
        with self.session() as session:
            return QueryDirector.build_queries(session, task, strategy=self)

    @property
    def is_read_only(self) -> bool:
        """Returns whether the strategy is read-only."""
        return False

    @abstractmethod
    def execute(self, task: ExtractMeta, export_to: Optional[Path] = None):
        """Execute the extraction task."""

    @abstractmethod
    def get_cohort(self, cohort: str) -> str:
        """Retrieve or create a cohort representation."""

    @abstractmethod
    def drop_cohort(self, cohort: str):
        """Drop a specific cohort table."""

    @abstractmethod
    def drop_table(self, name: str):
        """Drop a specific temporary table."""

    def cleanup(self):
        """Drop all remaining temporary tables and cohorts."""
        for table in list(self.created_tables):
            self.drop_table(table)
        self.drop_cohorts()

    def drop_cohorts(self):
        """Drop all remaining cohort tables."""
        for cohort in list(self.cohorts.keys()):
            self.drop_cohort(cohort)


class TempTableStrategy(ExecutionStrategy):
    """Strategy that uses temporary tables (requires write permissions)."""

    def execute(self, task: ExtractMeta, export_to: Optional[Path] = None):
        # pylint: disable=too-many-locals
        logging.info("EXTRACTING TABLE: %s", task.name)

        with self.session() as session:
            # TempTableStrategy is never read-only, so build_queries always
            # returns exactly one statement (no chunking ever occurs).
            (query,) = self.build_queries(task)
            temp_table = task.temp_name

            # Reflect source table to get column names and types
            source_table = Table(
                task.table,
                MetaData(),
                schema=task.schema,
                autoload_with=session.bind
            )

            # Initialize destination table
            meta = MetaData()
            dest_table = Table(temp_table, meta)

            # Add columns to destination table
            col_names = []
            for col in source_table.columns:
                dest_table.append_column(Column(col.name, col.type))
                col_names.append(col.name)

            # Create destination table
            self.created_tables.add(temp_table)
            self.cohorts[task.name] = temp_table
            dest_table.create(session.bind)

            # Load data into destination table
            session.execute(dest_table.insert().from_select(col_names, query))
            session.commit()

            if export_to:
                # Export to a CSV file
                export_path = export_to / f"{task.table}.csv.gz"
                self.csv_hashes[task.name] = DataExporter.stream_to_csv(
                    session,
                    [select(dest_table)],
                    export_path,
                    table_name=task.name
                )

    def drop_table(self, name: str):
        """Drop a specific temporary table."""
        if name not in self.created_tables:
            return

        with self.session() as session:
            try:
                sa_table = Table(name, MetaData())
                session.execute(DropTable(sa_table, if_exists=True))
                session.commit()
            except SQLAlchemyError as err:
                logging.warning("COULD NOT DROP TABLE '%s': %s", name, err)
            finally:
                self.created_tables.remove(name)

    # pylint: disable=too-many-locals
    def get_cohort(self, cohort: str) -> str:
        """Retrieve or create a cohort table."""
        if cohort in self.cohorts:
            return self.cohorts[cohort]

        logging.info("GENERATING COHORT DATA: %s", cohort)

        name = cohort.replace(".sql", "").replace(".", "_")
        name = f'tmp_{name}_{UUID}'

        if not cohort.endswith(".sql"):
            db_params = DatabaseMeta.load_env(self.directory)
            source_engine = Session.create(**db_params).get_bind()
            schema, table = cohort.split(".")

            # Initialize source table
            source_meta = MetaData()
            source_table = Table(
                table,
                source_meta,
                schema=schema,
                autoload_with=source_engine
            )

            dest_columns = []
            dest_engine = self.engine
            for column in source_table.columns:
                # Get column metadata
                dialect = dest_engine.dialect.name
                col_meta = MetadataExtractor.generate_column_meta(
                    column, dialect=dialect, compile_type=False
                )
                dest_columns.append(Column(
                    col_meta["name"], col_meta["dtype"][dialect]
                ))

            # Create destination table
            dest_meta = MetaData()
            dest_table = Table(name, dest_meta, *dest_columns)
            self.cohorts[cohort] = name
            dest_table.create(dest_engine)

            # Move data from source to destination
            cohort_data = pd.read_sql_table(table, source_engine, schema)
            cohort_data.to_sql(
                name, dest_engine, index=False, if_exists='append'
            )

            return name

        cohort_path = self.directory / 'extract' / cohort
        with open(cohort_path, encoding="utf-8") as file:
            query = text(file.read().strip())

        db_params = DatabaseMeta.load_env(self.directory)
        with Session.create(**db_params) as source_session:
            cohort_data = pd.read_sql_query(query, source_session.bind)

        self.cohorts[cohort] = name
        with self.session() as session:
            # Load cohort data and write to destination table
            cohort_data.to_sql(
                name, session.bind, index=False, if_exists='fail'
            )

        return name

    def drop_cohort(self, cohort: str):
        """Drop a specific cohort table."""
        if cohort in self.cohorts:
            logging.info("DROPPING COHORT DATA: %s", cohort)
            try:
                with self.session() as session:
                    sa_table = Table(self.cohorts[cohort], MetaData())
                    session.execute(DropTable(sa_table, if_exists=True))
                    session.commit()
            except SQLAlchemyError as err:
                logging.warning("COULD NOT DROP COHORT %s: %s", cohort, err)
            finally:
                del self.cohorts[cohort]


class JoinChain:
    """Join chain for filtering on read-only mode."""

    def __init__(self, filter_config: 'FilterConfig', engine):
        self._engine = engine
        self._cohort_cols = filter_config.cohort_columns
        self._cohort_alias = filter_config.reference.alias
        self._target_cols = []
        for c in filter_config.reference.columns:
            self._target_cols.append(text(c))
        self._cohort_types = {}
        self._seed_joins = []
        self._where_clauses = []
        self._from_clause = None

    @classmethod
    def build(cls, filter_config: 'FilterConfig', engine) -> "JoinChain":
        """Build and return a fully resolved chain for *filter_config*.

        :param filter_config: Filter configuration describing the join chain.
        :type filter_config: FilterConfig

        :param engine: SQLAlchemy engine used to reflect remote tables.

        :return: Populated :class:`JoinChain` ready to execute seed chunks.
        :rtype: JoinChain
        """
        chain_ = cls(filter_config, engine)
        for j_conf in filter_config.join:
            chain_._extend(j_conf)
        return chain_

    def _extend(self, j_conf):
        """Reflect one join config and extend the SA chain."""
        ref = j_conf.reference
        sa_table = Table(
            ref.table, MetaData(), schema=ref.schema,
            autoload_with=self._engine,
        ).alias(ref.alias)

        # Retrieve column types and seed joins from the filter config
        for pair in j_conf.seed_pairs():
            col = sa_table.c[pair.remote]
            self._cohort_types[pair.local] = col.type
            self._seed_joins.append((col, pair.local))

        # Retrieve constant filters from the filter config
        for p in j_conf.const_filters():
            where = sa_table.c[p.remote] == literal(p.value)
            self._where_clauses.append(where)

        # Build the FROM clause for the join chain
        if self._from_clause is None:
            self._from_clause = sa_table
        else:
            on_clauses = []
            for p in j_conf.cross_pairs():
                on_clauses.append(text(f"{p.left} = {p.right}"))
            onclause = and_(*on_clauses) if on_clauses else text("1=1")
            self._from_clause = self._from_clause.join(sa_table, onclause)

    def execute(self, chunk: List, session) -> List:
        """Execute a seed chunk against the built chain.

        :param chunk: Batch of seed tuples to look up.
        :type chunk: list

        :param session: Active SQLAlchemy session.

        :return: All matching rows from the remote tables.
        :rtype: list
        """
        # Create list of column objects for VALUES clause
        val_cols = []
        for name in self._cohort_cols:
            column = sa_column(name, self._cohort_types.get(name, String()))
            val_cols.append(column)
        v = values(*val_cols, name=self._cohort_alias).data(chunk)

        # Build join ON clauses from seed pairs
        seed_on = []
        for col, name in self._seed_joins:
            seed_on.append(col == v.c[name])

        # Build the full statement
        stmt = select(*self._target_cols).select_from(
            self._from_clause.join(v, and_(*seed_on))
        ).distinct()
        if self._where_clauses:
            stmt = stmt.where(and_(*self._where_clauses))
        return session.execute(stmt).all()


class StreamStrategy(ExecutionStrategy):
    """Strategy that streams data directly from source (read-only)."""

    @property
    def is_read_only(self) -> bool:
        return True

    def execute(self, task: ExtractMeta, export_to: Optional[Path] = None):
        """Execute extraction by streaming and filtering in memory."""
        if not export_to:
            export_to = self.directory / "data" / ".tmp"
            export_to.mkdir(exist_ok=True)
            self.created_tables.add(task.temp_name)

        logging.info("EXTRACTING TABLE: %s", task.name)

        with self.session() as session:
            # Export to a CSV file
            queries = self.build_queries(task)
            export_path = export_to / f"{task.table}.csv.gz"
            self.csv_hashes[task.name] = DataExporter.stream_to_csv(
                session,
                queries,
                export_path,
                table_name=task.table
            )

    def drop_table(self, name: str):
        """Clean up temporary files."""
        if name not in self.created_tables:
            return

        table_name = name[4:-(len(UUID) + 1)]
        path = self.directory / "data" / ".tmp" / f"{table_name}.csv.gz"
        if path.exists():
            path.unlink()
        self.created_tables.remove(name)

        # Try to remove the temp directory itself if empty
        temp_dir = self.directory / "data" / ".tmp"
        if temp_dir.exists() and not any(temp_dir.iterdir()):
            temp_dir.rmdir()

    def drop_cohort(self, cohort: str):
        """Clear cohort from memory (no DDL in stream mode)."""
        if cohort in self.cohorts:
            logging.info("DROPPING COHORT DATA: %s", cohort)
            del self.cohorts[cohort]

    def get_cohort(self, cohort: str) -> Any:
        """Register the cohort but do not load data yet."""
        if cohort in self.cohorts:
            return self.cohorts[cohort]

        if cohort.endswith(".sql"):
            cohort_path = self.directory / 'extract' / cohort
            name = cohort.replace(".sql", "")
            with open(cohort_path, encoding="utf-8") as file:
                query = file.read().strip()
                subquery = f"(\n  {query}\n) {name}"
                self.cohorts[cohort] = subquery
                return subquery

        self.cohorts[cohort] = cohort
        return cohort

    def get_cohort_set(
        self,
        filter_config: FilterConfig,
        chunk_size: int = 10000
    ) -> List[Tuple[str, ...]]:
        """Load cohort data into memory.

        :param filter_config: Filter configuration.
        :type filter_config: FilterConfig

        :param chunk_size: Size of chunks to retrieve.
        :type chunk_size: int

        :return: List of cohort records.
        :rtype: list[tuple[str, ...]]
        """
        seed_chunks = self._fetch_local_seeds(
            filter_config,
            chunk_size=chunk_size
        )

        # If no joins, no need to query the database
        if not filter_config.join:
            return list(dict.fromkeys(chain.from_iterable(seed_chunks)))

        chain_obj = JoinChain.build(filter_config, self.engine)
        final_tuples = set()
        for chunk in seed_chunks:
            with self.session() as session:
                for row in chain_obj.execute(chunk, session):
                    str_vals = []
                    for val in row:
                        str_vals.append(str(val))
                    final_tuples.add(tuple(str_vals))
        return list(final_tuples)

    def _fetch_local_seeds(
        self,
        filter_config: FilterConfig,
        chunk_size: int = 10000
    ) -> Iterator[List[Tuple[str, ...]]]:
        # pylint: disable=too-many-locals
        """Fetch the initial set of IDs from the local cohort table or CSV."""
        fetch_cols = filter_config.cohort_columns
        cohort = filter_config.resolved_cohort

        # Check if the cohort is an extraction task in the graph
        if self.graph and cohort in self.graph.tasks:
            # Find the file path
            table = self.graph.tasks[cohort].table
            for csv_path in (self.directory / "data").rglob(f"{table}.csv.gz"):
                break
            else:
                raise FileNotFoundError(f"Missing file for '{cohort}'")

            # Read and hash the compressed file on-the-fly
            with open(csv_path, 'rb') as f:
                reader = HashingReader(f)
                chunks = pd.read_csv(
                    reader,
                    compression='gzip',
                    usecols=fetch_cols,
                    dtype=str,
                    chunksize=chunk_size
                )

                # Load seeds in chunks
                for chunk in chunks:
                    chunk = chunk[fetch_cols]
                    unique = chunk.drop_duplicates()
                    yield list(unique.itertuples(index=False, name=None))

            # Validate hash to prevent corrupted reads
            if reader.hasher.hexdigest() != self.csv_hashes[cohort]:
                raise ValueError(f"Hash mismatch for file '{csv_path.name}'")

        else:
            # Fallback to local cohort database query
            db_params = DatabaseMeta.load_env(self.directory)
            with Session.create(**db_params) as session:
                schema, table_name = cohort.split(".")
                sa_table = Table(
                    table_name,
                    MetaData(),
                    schema=schema,
                    autoload_with=session.bind
                )
                cols = [sa_table.c[c] for c in fetch_cols]
                stmt = select(*cols).distinct()
                res = session.execute(stmt)
                for partition in res.partitions(chunk_size):
                    yield [tuple(str(v) for v in row) for row in partition]


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
        self.ref_counts = graph.dependents.count()
        self.dependents = graph.dependents.as_list()

        # graphlib topological sorter for parallel orchestration
        self.ts = graph.topological_sorter
        self.live_tasks = {}

    def execute(
        self,
        execute: Optional[Set[str]] = None,
        export: Optional[Set[str]] = None,
        data_dir: Optional[Path] = None
    ):
        """Run tasks in parallel using TopologicalSorter interactive mode."""
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {}

            while self.ts.is_active():
                # Submit all ready tasks
                for name in self.ts.get_ready():
                    task = executor.submit(
                        self._run_task,
                        name,
                        execute,
                        export,
                        data_dir
                    )
                    futures[task] = name

                if not futures:
                    # This shouldn't happen if is_active is true,
                    # unless there's a deadlock or we're waiting for futures.
                    if any(f.running() for f in futures):
                        pass
                    else:
                        break

                # Wait for at least one task to complete
                for future in as_completed(futures):
                    task_name = futures.pop(future)
                    try:
                        future.result()
                        with self.lock:
                            self.ts.done(task_name)
                            # Trigger cleanup and get new ready tasks
                            self._on_task_complete(task_name)
                        break  # Go back to the loop to submit new ready tasks
                    except (SQLAlchemyError, OSError) as e:
                        logging.error("FAILED TASK %s: %s", task_name, e)
                        # Mark it as done anyway to avoid deadlocking the
                        # sorter, or handle failure accordingly
                        with self.lock:
                            self.ts.done(task_name)
                        break

    def _run_task(
        self,
        name: str,
        execute: Optional[Set[str]],
        export: Optional[Set[str]],
        data_dir: Optional[Path]
    ):
        """Execute a task (table extraction or cohort initialization)."""
        if execute is not None and name not in execute:
            return

        # Start a managed session for this task
        with self.strategy.session():
            # Table extraction task
            if name in self.graph.tasks:
                task = self.graph.tasks[name]
                should_export = export is None or task.name in export
                export_to = data_dir if should_export else None

                with self.lock:
                    self.live_tasks[name] = task

                self.strategy.execute(task, export_to=export_to)
            # Cohort node
            else:
                self.strategy.get_cohort(name)

    def _on_task_complete(self, name: str):
        """Perform cleanup for dependencies no longer needed."""
        to_drop_cohorts = []
        to_drop_tables = []

        with self.lock:
            # Retrieve dependencies for the node that just finished
            if name in self.graph.tasks:
                task = self.graph.tasks[name]
                deps = task.dependencies()
            else:
                # Cohorts currently have no prerequisites in the graph
                deps = set()
                task = None

            for dep in deps:
                # Decrement ref counts of dependencies used by THIS node
                self.ref_counts[dep] -= 1

                # If no other nodes depend on this dependency, drop it
                if self.ref_counts[dep] == 0:
                    if dep in self.strategy.cohorts:
                        to_drop_cohorts.append(dep)
                    elif dep in self.live_tasks:
                        to_drop_tables.append(self.live_tasks[dep].temp_name)
                        del self.live_tasks[dep]

            # Check if THIS node itself is no longer needed by anyone else
            if self.ref_counts.get(name, 0) == 0:
                if task:
                    to_drop_tables.append(task.temp_name)
                    if name in self.live_tasks:
                        del self.live_tasks[name]
                elif name in self.strategy.cohorts:
                    to_drop_cohorts.append(name)

        # Execute drops outside of the lock
        for dep in to_drop_cohorts:
            self.strategy.drop_cohort(dep)
        for table in to_drop_tables:
            self.strategy.drop_table(table)
