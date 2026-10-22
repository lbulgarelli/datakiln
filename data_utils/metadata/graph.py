"""
Dependency graphs for metadata objects.
"""
from pathlib import Path
from functools import cached_property
from typing import Dict, List, Set
from graphlib import TopologicalSorter, CycleError

import sqlglot
from sqlglot import exp


DIALECT_MAP = {
    "postgresql": "postgres",
    "mssql": "tsql"
}


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

    Nodes declare the database objects (schema.table) they `provides` and
    `requires`. A node depends on the nodes that provide the objects it
    requires. Objects that no node provides (e.g. tables loaded elsewhere)
    are ignored, unless `keep_unresolved` is set, in which case they become
    dependencies that are not part of the nodes.

    Nodes are kept in the order they are given and that order is preserved
    whenever the dependencies allow it.
    """

    def __init__(self, nodes: Dict[str, object],
                 keep_unresolved: bool = False):
        """
        :param nodes: Nodes with `provides` and `requires`, by identifier.
        :type nodes: dict[str, object]

        :param keep_unresolved: Keep required objects that no node provides
                                as dependencies.
        :type keep_unresolved: bool
        """
        self.nodes = nodes

        # Map objects to the nodes that provide them
        providers: Dict[str, Set[str]] = {}
        for name, node in nodes.items():
            for obj in node.provides:
                providers.setdefault(obj, set()).add(name)

        self.adj: Dict[str, Set[str]] = {}
        for name, node in nodes.items():
            self.adj[name] = set()
            for obj in node.requires:
                if obj in providers:
                    self.adj[name] |= providers[obj]
                elif keep_unresolved:
                    self.adj[name].add(obj)

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
        position = {name: idx for idx, name in enumerate(self.nodes)}
        sorter = self.topological_sorter

        ordered = []
        while sorter.is_active():
            ready = sorted(
                sorter.get_ready(),
                key=lambda name: (position.get(name, -1), name)
            )
            for name in ready:
                if name in self.nodes:
                    ordered.append(self.nodes[name])
                sorter.done(name)
        return ordered


class ScriptMeta:
    # pylint: disable=too-few-public-methods
    """SQL script and the database objects it creates and uses."""

    # Statement kinds that create an object other statements can reference
    _creates = {"TABLE", "VIEW"}

    def __init__(self, path: Path, sql: str, dialect: str = "postgresql"):
        """
        :param path: Path of the script.
        :type path: Path

        :param sql: Content of the script.
        :type sql: str

        :param dialect: SQL dialect of the script.
        :type dialect: str
        """
        self.path = Path(path)
        self.sql = sql
        self.provides: Set[str] = set()
        self.requires: Set[str] = set()

        read = DIALECT_MAP.get(dialect, dialect)
        for statement in sqlglot.parse(sql, read=read):
            if statement is not None:
                self._inspect(statement)
        self.requires -= self.provides

    @property
    def name(self) -> str:
        """Returns the script identifier (path)."""
        return str(self.path)

    @staticmethod
    def _full_name(table: exp.Table) -> str:
        """Returns schema.table, or an empty string if unqualified."""
        return f"{table.db}.{table.name}" if table.db and table.name else ""

    def _inspect(self, statement: exp.Expression):
        """Collect the objects created and referenced by a statement."""
        # Dropping an object does not require it to exist
        if isinstance(statement, exp.Drop):
            return

        created = None
        if isinstance(statement, exp.Create):
            kind = (statement.kind or "").upper()
            target = statement.this
            if kind in self._creates and isinstance(target, exp.Table):
                created = target
                self.provides.add(self._full_name(target))

        # Names defined by WITH clauses are not database objects
        ctes = {cte.alias for cte in statement.find_all(exp.CTE)}
        for table in statement.find_all(exp.Table):
            if table is created or (not table.db and table.name in ctes):
                continue
            if name := self._full_name(table):
                self.requires.add(name)


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
        for script in scripts:
            if script.name in nodes:
                raise ValueError(f"Duplicate node: {script.name}")
            nodes[script.name] = script
        super().__init__(nodes)

    @property
    def tables(self) -> List:
        """Tables, ordered so that referenced tables come first."""
        return [n for n in self.sort() if not isinstance(n, ScriptMeta)]

    @property
    def scripts(self) -> List[ScriptMeta]:
        """Scripts, ordered so that the scripts creating the objects they
        use come first."""
        return [n for n in self.sort() if isinstance(n, ScriptMeta)]
