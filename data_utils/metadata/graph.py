"""
Dependency graphs for metadata objects.
"""
from functools import cached_property
from typing import Dict, List, Optional, Set
from graphlib import TopologicalSorter, CycleError

from data_utils.metadata.script import ScriptMeta, ScriptError
from data_utils.metadata.table import TableMeta


class DependentsMap(dict):
    """Successor map from adjacency map."""

    def __init__(self, adj: Dict[str, Set[str]]):
        super().__init__()
        for node, predecessors in adj.items():
            for pred in predecessors:
                self.setdefault(pred, set()).add(node)

    def count(self) -> Dict[str, int]:
        """Returns the reference counts for each node."""
        return {node: len(deps) for node, deps in self.items()}

    def as_list(self) -> Dict[str, List[str]]:
        """Returns the dependents map with sets converted to lists."""
        return {node: list(deps) for node, deps in self.items()}


class DependencyGraph:
    # pylint: disable=too-few-public-methods
    """Directed Acyclic Graph for nodes that depend on one another.

    Nodes list the names of the nodes they `requires`. Required names that are
    not nodes (e.g. tables loaded elsewhere) are ignored, unless
    `keep_unresolved` is set, in which case they become dependencies that are
    not part of the nodes.

    Nodes that become ready at the same time are ordered by name, so the order
    does not depend on the order the nodes are given.
    """

    def __init__(self, nodes: Dict[str, object],
                 keep_unresolved: bool = False,
                 aliases: Optional[Dict[str, Set[str]]] = None):
        """
        :param nodes: Nodes with `requires`, by name.
        :type nodes: dict[str, object]

        :param keep_unresolved: Keep required names that are not nodes as
                                dependencies.
        :type keep_unresolved: bool

        :param aliases: Names that stand for several nodes, which are all
                        dependencies of the nodes requiring the name. A node
                        is never a dependency of itself this way.
        :type aliases: dict[str, set[str]]
        """
        self.nodes = nodes
        aliases = aliases or {}

        self.adj: Dict[str, Set[str]] = {}
        for name, node in nodes.items():
            self.adj[name] = set()
            for required in node.requires:
                if required in nodes:
                    self.adj[name].add(required)
                elif required in aliases:
                    self.adj[name] |= aliases[required] - {name}
                elif keep_unresolved:
                    self.adj[name].add(required)

    @property
    def topological_sorter(self) -> TopologicalSorter:
        """Returns a prepared TopologicalSorter for the graph.

        :raises ValueError: If the dependencies contain a cycle.
        """
        sorter = TopologicalSorter(self.adj)
        try:
            sorter.prepare()
        except CycleError as error:
            cycle = " -> ".join(error.args[1])
            raise ValueError(f"Circular dependency: {cycle}") from error
        return sorter

    @cached_property
    def dependents(self) -> DependentsMap:
        """Map each node to the set of nodes that depend on it."""
        return DependentsMap(self.adj)

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
            for name in sorted(sorter.get_ready()):
                if name in self.nodes:
                    ordered.append(self.nodes[name])
                sorter.done(name)
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
        nodes = {table.full_name: table for table in tables}

        # Calls do not tell which overload of a function is used, so they
        # refer to all of them
        overloads: Dict[str, Set[str]] = {}
        for script in scripts:
            if other := nodes.get(script.full_name):
                creator = getattr(other, "path", "the metadata")
                raise ScriptError(script.path, f"duplicate in {creator}.")
            nodes[script.full_name] = script
            function, parenthesis, _ = script.full_name.partition("(")
            if parenthesis:
                overloads.setdefault(function, set()).add(script.full_name)
        super().__init__(nodes, aliases=overloads)

    @property
    def tables(self) -> List:
        """Tables, ordered so that referenced tables come first."""
        return [n for n in self.sort() if isinstance(n, TableMeta)]

    @property
    def scripts(self) -> List[ScriptMeta]:
        """Scripts, ordered so that the scripts creating the objects they
        use come first."""
        return [n for n in self.sort() if isinstance(n, ScriptMeta)]
