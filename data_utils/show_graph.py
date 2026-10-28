"""
Shows the dependency graph of the tables and derive scripts.
"""
from argparse import ArgumentParser
from operator import attrgetter
from pathlib import Path
from typing import Iterable, List

from data_utils.metadata.database import DatabaseMeta
from data_utils.metadata.graph import DatabaseGraph
from data_utils.metadata.table import TableMeta


TABLE_STYLE = "fill:#e8f1fb,stroke:#4a78b5,color:#1f3b5c"
SCRIPT_STYLE = "fill:#fdf1dc,stroke:#c98a2b,color:#5c3d0e"


def is_schema(node) -> bool:
    """Whether the node is a script that creates a schema.

    :param node: Table or script.
    :type node: data_utils.metadata.table.TableMeta |
                data_utils.metadata.script.ScriptMeta

    :rtype: bool
    """
    return "." not in node.full_name and "(" not in node.full_name


def label(node) -> List[str]:
    """Lines to tell a node by: its name, and the file of a script if the
    name does not tell it.

    :param node: Table or script.
    :type node: data_utils.metadata.table.TableMeta |
                data_utils.metadata.script.ScriptMeta

    :rtype: list[str]
    """
    lines = [node.full_name]
    if not isinstance(node, TableMeta):
        if node.path.stem != node.full_name.rpartition(".")[2]:
            lines.append(str(node.path))
    return lines


def edges(graph: DatabaseGraph, nodes: Iterable) -> List[tuple]:
    """Pairs of two tables or two scripts where the second one needs the
    first. There are none between a table and a script.

    :param graph: Dependency graph.
    :type graph: data_utils.metadata.graph.DatabaseGraph

    :param nodes: Nodes that can be shown.
    :type nodes: Iterable

    :rtype: list[tuple]
    """
    candidates = set(nodes)
    pairs = []
    for node in sorted(candidates, key=attrgetter("full_name")):
        for required in sorted(graph.adj[node], key=attrgetter("full_name")):
            if required in candidates and (
                isinstance(required, TableMeta) == isinstance(node, TableMeta)
            ):
                pairs.append((required, node))
    return pairs


def select(graph: DatabaseGraph, include: Iterable[str] = None,
           schemas: bool = False) -> List:
    """Nodes to show, ordered by name: the ones that have an arrow, as the
    others would only be a row of boxes, and the ones asked for.

    :param graph: Dependency graph.
    :type graph: data_utils.metadata.graph.DatabaseGraph

    :param include: Names of the nodes to show, with the nodes they need. All
                    by default.
    :type include: Iterable[str]

    :param schemas: Whether to show the scripts that create schemas, which
                    every script of the schema needs.
    :type schemas: bool

    :raises ValueError: If any of the names is not a node.

    :rtype: list
    """
    nodes = set(graph.nodes.values())
    asked = set()
    if include:
        asked = graph.find(include)
        nodes = graph.requirements(asked)

    candidates = set()
    for node in nodes:
        if schemas or not is_schema(node):
            candidates.add(node)

    connected = asked & candidates
    for required, node in edges(graph, candidates):
        connected.update((required, node))
    return sorted(connected, key=attrgetter("full_name"))


def mermaid(graph: DatabaseGraph, nodes: List, direction: str = "LR") -> str:
    """Graph as a Mermaid flowchart, which GitLab and GitHub draw in a
    Markdown file.

    :param graph: Dependency graph.
    :type graph: data_utils.metadata.graph.DatabaseGraph

    :param nodes: Nodes shown, in order.
    :type nodes: list

    :param direction: Direction of the flow, LR or TB.
    :type direction: str

    :rtype: str
    """
    ids = {}
    lines = [f"flowchart {direction}"]
    for node in nodes:
        ids[node] = f"n{len(ids)}"
        text = "<br/>".join(label(node)).replace('"', "#quot;")
        if isinstance(node, TableMeta):
            lines.append(f'    {ids[node]}[("{text}")]')
        else:
            lines.append(f'    {ids[node]}["{text}"]')

    for required, node in edges(graph, nodes):
        lines.append(f"    {ids[required]} --> {ids[node]}")

    lines.append(f"    classDef table {TABLE_STYLE}")
    lines.append(f"    classDef script {SCRIPT_STYLE}")
    for kind, style in ((True, "table"), (False, "script")):
        members = [ids[n] for n in nodes if isinstance(n, TableMeta) == kind]
        if members:
            lines.append(f"    class {','.join(members)} {style}")
    return "\n".join(lines) + "\n"


def dot(graph: DatabaseGraph, nodes: List, direction: str = "LR") -> str:
    """Graph in the DOT language, which Graphviz draws.

    :param graph: Dependency graph.
    :type graph: data_utils.metadata.graph.DatabaseGraph

    :param nodes: Nodes shown, in order.
    :type nodes: list

    :param direction: Direction of the flow, LR or TB.
    :type direction: str

    :rtype: str
    """
    ids = {}
    lines = ["digraph dependencies {", f"    rankdir={direction};",
             '    node [style=filled, fontname="Helvetica"];']
    for node in nodes:
        ids[node] = f"n{len(ids)}"
        text = "\\n".join(label(node)).replace('"', '\\"')
        if isinstance(node, TableMeta):
            attributes = 'shape=cylinder, fillcolor="#e8f1fb"'
        else:
            attributes = 'shape=box, fillcolor="#fdf1dc"'
        lines.append(f'    {ids[node]} [label="{text}", {attributes}];')

    for required, node in edges(graph, nodes):
        lines.append(f"    {ids[required]} -> {ids[node]};")
    lines.append("}")
    return "\n".join(lines) + "\n"


FORMATS = {"mermaid": mermaid, "dot": dot}


def main(args=None):
    """Shows the dependency graph of the tables and derive scripts."""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("directory", metavar="directory", type=str,
                        help="Path of the directory with the dataset "
                             "metadata.")
    parser.add_argument("--format", choices=sorted(FORMATS),
                        default="mermaid",
                        help="Format of the graph: Mermaid, which GitLab "
                             "draws in Markdown, or DOT, for Graphviz.")
    parser.add_argument("--direction", choices=["LR", "TB"], default="LR",
                        help="Direction of the flow: left to right, or top "
                             "to bottom.")
    parser.add_argument("--include", metavar="name", type=str, nargs="+",
                        help="Show only these tables and scripts, and what "
                             "they need")
    parser.add_argument("--schemas", action="store_true",
                        help="Show the scripts that create schemas, and "
                             "the arrows from them")
    parser.add_argument("--output", metavar="file", type=str,
                        help="File to write the graph to, instead of "
                             "standard output")
    args = parser.parse_args(args)

    directory = Path(args.directory).absolute()
    name = DatabaseMeta.load_env(directory, required=False)["database"]
    graph = DatabaseMeta(directory, name).graph

    nodes = select(graph, args.include, args.schemas)
    text = FORMATS[args.format](graph, nodes, args.direction)
    if args.output:
        Path(args.output).write_text(text, encoding="utf-8")
    else:
        print(text, end="")


if __name__ == "__main__":
    main()
