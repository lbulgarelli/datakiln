"""
Dependency graphs for metadata objects.
"""
from dataclasses import dataclass
from functools import cached_property
from operator import attrgetter
from typing import Dict, Iterable, List, Optional, Set
from graphlib import TopologicalSorter, CycleError

from data_utils.metadata.script import ScriptMeta, ScriptError
from data_utils.metadata.table import TableMeta


class DependentsMap(dict):
    """Successor map from adjacency map."""

    def __init__(self, adj: Dict[object, Set]):
        super().__init__()
        for node, predecessors in adj.items():
            for pred in predecessors:
                self.setdefault(pred, set()).add(node)

    def count(self) -> Dict[object, int]:
        """Returns the reference counts for each node."""
        return {node: len(deps) for node, deps in self.items()}

    def as_list(self) -> Dict[object, List]:
        """Returns the dependents map with sets converted to lists."""
        return {node: list(deps) for node, deps in self.items()}


@dataclass(frozen=True)
class Unresolved:
    """Name that nodes require, which is not a node. Two with the same name are
    the same dependency."""

    full_name: str


class DependencyGraph:
    # pylint: disable=too-few-public-methods
    """Directed Acyclic Graph for nodes that depend on one another.

    Nodes list the names of the nodes they `requires`, which the graph turns
    into the nodes themselves. Required names that are not nodes (e.g. tables
    loaded elsewhere) are ignored, unless `keep_unresolved` is set, in which
    case they become dependencies of the nodes that are `Unresolved` objects,
    which are not part of the nodes.

    Nodes have a `full_name`, which tells them apart in errors.
    """

    def __init__(self, nodes: List,
                 keep_unresolved: bool = False,
                 aliases: Optional[Dict[str, Set]] = None):
        """
        :param nodes: Nodes with a `full_name` and what they `requires`.
        :type nodes: list[object]

        :param keep_unresolved: Keep required names that are not nodes as
                                dependencies.
        :type keep_unresolved: bool

        :param aliases: Names that stand for several nodes, which are all
                        dependencies of the nodes requiring the name. A node
                        is never a dependency of itself this way.
        :type aliases: dict[str, set[object]]
        """
        # By name, which is how what the nodes require is found
        self.nodes: Dict[str, object] = {}
        for node in nodes:
            self.nodes[node.full_name] = node
        aliases = aliases or {}

        self.unresolved: Set[Unresolved] = set()

        self.adj: Dict[object, Set] = {}
        for node in self.nodes.values():
            self.adj[node] = set()
            for required in node.requires:
                if required in self.nodes:
                    self.adj[node].add(self.nodes[required])
                elif required in aliases:
                    self.adj[node] |= aliases[required] - {node}
                elif keep_unresolved:
                    unresolved = Unresolved(required)
                    self.unresolved.add(unresolved)
                    self.adj[node].add(unresolved)

    @property
    def topological_sorter(self) -> TopologicalSorter:
        """Returns a prepared TopologicalSorter for the graph.

        :raises ValueError: If the dependencies contain a cycle.
        """
        sorter = TopologicalSorter(self.adj)
        try:
            sorter.prepare()
        except CycleError as error:
            cycle = " -> ".join(map(attrgetter("full_name"), error.args[1]))
            raise ValueError(f"Circular dependency: {cycle}") from error
        return sorter

    @cached_property
    def dependents(self) -> DependentsMap:
        """Map each node to the set of nodes that depend on it."""
        return DependentsMap(self.adj)

    @staticmethod
    def _reach(nodes: Iterable, edges: Dict[object, Iterable]) -> Set:
        """Nodes adjacent to the specified nodes by the edges, directly or
        indirectly."""
        reached: Set = set()
        queue = list(nodes)
        while queue:
            for other in edges.get(queue.pop(), ()):
                if other not in reached:
                    reached.add(other)
                    queue.append(other)
        return reached

    def find(self, names: Iterable[str]) -> Set:
        """Nodes by name.

        :param names: Names of nodes of the graph.
        :type names: Iterable[str]

        :return: The nodes.
        :rtype: set[object]

        :raises ValueError: If any of the names is not a node.
        """
        names = list(names)
        unknown = set(names) - set(self.nodes)
        if unknown:
            raise ValueError(f"Not found: {', '.join(sorted(unknown))}.")

        found = set()
        for name in names:
            found.add(self.nodes[name])
        return found

    def requirements(self, nodes: Iterable) -> Set:
        """The specified nodes, with the nodes they need, directly or
        indirectly. Names that are not nodes are left out.

        :param nodes: Nodes of the graph.
        :type nodes: Iterable[object]

        :return: The nodes and what they need.
        :rtype: set[object]
        """
        nodes = list(nodes)
        needed = set(nodes) | self._reach(nodes, self.adj)
        return needed - self.unresolved

    def affected(self, nodes: Iterable) -> Set:
        """Nodes that need any of the specified nodes, directly or
        indirectly.

        :param nodes: Nodes of the graph.
        :type nodes: Iterable[object]

        :return: The nodes that need them.
        :rtype: set[object]
        """
        return self._reach(nodes, self.dependents)

    def sort(self) -> List:
        """Perform topological sort to determine execution order.

        :return: Nodes ordered so that dependencies come first. Dependencies
                 that are not nodes are not returned.
        :rtype: list

        :raises ValueError: If the dependencies contain a cycle.
        """
        sorter = self.topological_sorter

        ordered = []
        while sorter.is_active():
            for node in sorter.get_ready():
                if not isinstance(node, Unresolved):
                    ordered.append(node)
                sorter.done(node)
        return ordered


class DatabaseGraph(DependencyGraph):
    # pylint: disable=too-few-public-methods
    """Dependency graph for the tables (metadata) and the scripts (derive)
    of a database."""

    def __init__(self, tables: List, scripts: List[ScriptMeta]):
        """
        :param tables: Table metadata.
        :type tables: list[data_utils.metadata.table.TableMeta]

        :param scripts: Scripts that create objects from the tables.
        :type scripts: list[ScriptMeta]
        """
        created = {table.full_name: table for table in tables}
        overloads: Dict[str, Set] = {}
        for script in scripts:
            # An object can only be created once
            if other := created.get(script.full_name):
                creator = getattr(other, "path", "the metadata")
                raise ScriptError(script.path, f"duplicate in {creator}.")
            created[script.full_name] = script

            # Calls do not tell which overload of a function is used, so they
            # refer to all of them
            function, parenthesis, _ = script.full_name.partition("(")
            if parenthesis:
                overloads.setdefault(function, set()).add(script)

        super().__init__(list(created.values()), aliases=overloads)

    @property
    def tables(self) -> List:
        """Tables, ordered so that referenced tables come first."""
        return [n for n in self.sort() if isinstance(n, TableMeta)]

    @property
    def scripts(self) -> List[ScriptMeta]:
        """Scripts, ordered so that the scripts creating the objects they
        use come first."""
        return [n for n in self.sort() if isinstance(n, ScriptMeta)]
