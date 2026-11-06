"""
Dependency graphs for metadata objects.
"""
from dataclasses import dataclass
from functools import cached_property
from typing import Dict, Iterable, List, Optional, Set
from graphlib import TopologicalSorter

import networkx as nx

from datakiln.metadata.script import ScriptMeta, ScriptError
from datakiln.metadata.table import TableMeta


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

        # An arrow goes from what is needed to what needs it
        self.digraph = nx.DiGraph()
        self.digraph.add_nodes_from(self.nodes.values())
        for node in self.nodes.values():
            for required in node.requires:
                if required in self.nodes:
                    self.digraph.add_edge(self.nodes[required], node)
                elif required in aliases:
                    for other in aliases[required] - {node}:
                        self.digraph.add_edge(other, node)
                elif keep_unresolved:
                    unresolved = Unresolved(required)
                    self.unresolved.add(unresolved)
                    self.digraph.add_edge(unresolved, node)

    def dependencies(self, node: object) -> Set:
        """Nodes that a node needs directly.

        :param node: Node of the graph.
        :type node: object

        :return: The nodes it needs, which include the dependencies that are
                 not nodes if they are kept.
        :rtype: set[object]
        """
        return set(self.digraph.predecessors(node))

    def _check_acyclic(self):
        """Checks that no node needs itself, directly or indirectly.

        :raises ValueError: If the dependencies contain a cycle.
        """
        if nx.is_directed_acyclic_graph(self.digraph):
            return

        # Read the arrows the other way, from a node to what it needs
        cycle = nx.find_cycle(self.digraph.reverse(copy=False))
        path = []
        for node, _ in cycle:
            path.append(node.full_name)
        path.append(path[0])
        raise ValueError(f"Circular dependency: {' -> '.join(path)}")

    @property
    def topological_sorter(self) -> TopologicalSorter:
        """Returns a prepared TopologicalSorter for the graph, which gives the
        nodes that can run as the ones they need are done.

        :raises ValueError: If the dependencies contain a cycle.
        """
        self._check_acyclic()

        sorter = TopologicalSorter()
        for node in self.digraph:
            sorter.add(node, *self.digraph.predecessors(node))
        sorter.prepare()
        return sorter

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
        needed = set()
        for node in nodes:
            needed.add(node)
            needed |= nx.ancestors(self.digraph, node)
        return needed - self.unresolved

    def affected(self, nodes: Iterable) -> Set:
        """Nodes that need any of the specified nodes, directly or
        indirectly.

        :param nodes: Nodes of the graph.
        :type nodes: Iterable[object]

        :return: The nodes that need them.
        :rtype: set[object]
        """
        affected = set()
        for node in nodes:
            affected |= nx.descendants(self.digraph, node)
        return affected

    def sort(self) -> List:
        """Perform topological sort to determine execution order.

        :return: Nodes ordered so that dependencies come first. Dependencies
                 that are not nodes are not returned.
        :rtype: list

        :raises ValueError: If the dependencies contain a cycle.
        """
        self._check_acyclic()

        ordered = []
        for node in nx.topological_sort(self.digraph):
            if not isinstance(node, Unresolved):
                ordered.append(node)
        return ordered


class DatabaseGraph(DependencyGraph):
    # pylint: disable=too-few-public-methods
    """Dependency graph for the tables (metadata) and the scripts (derive)
    of a database."""

    def __init__(self, tables: List, scripts: List[ScriptMeta]):
        """
        :param tables: Table metadata.
        :type tables: list[datakiln.metadata.table.TableMeta]

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

    @cached_property
    def schemas(self) -> Set[str]:
        """Schemas of the tables and scripts, in lower case. A function is in
        none."""
        schemas = set()
        for name in self.nodes:
            if "(" not in name:
                schemas.add(name.partition(".")[0])
        return schemas

    @property
    def tables(self) -> List:
        """Tables, ordered so that referenced tables come first."""
        return [n for n in self.sort() if isinstance(n, TableMeta)]

    @property
    def scripts(self) -> List[ScriptMeta]:
        """Scripts, ordered so that the scripts creating the objects they
        use come first."""
        return [n for n in self.sort() if isinstance(n, ScriptMeta)]
