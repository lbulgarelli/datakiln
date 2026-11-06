"""
Command line of datakiln, which builds a database from a repository of files.
"""
from argparse import ArgumentParser
from typing import List, Optional

from datakiln import (
    build_database, build_dbms, build_metadata, build_redcap, show_graph
)
from datakiln.arguments import setup_logging


# Name of each command, with what runs it, what it does and the log it writes
COMMANDS = [
    ("build", build_database, "Build the database from the repository, "
     "loading only what changed.", "build.log"),
    ("metadata", build_metadata, "Generate the metadata of the tables from "
     "the data files.", "metadata.log"),
    ("graph", show_graph, "Show what the tables and scripts depend on.", None),
]

# The same for the commands that read from a source
SOURCES = [
    ("dbms", build_dbms, "Extract data, and optionally the metadata of the "
     "tables, from a source database."),
    ("redcap", build_redcap, "Extract data, and optionally the metadata of "
     "the tables, from a REDCap project."),
]


def get_parser() -> ArgumentParser:
    """Builds the parser of the commands.

    :return: Parser, whose result has the function that runs the command and
             the log it writes.
    :rtype: argparse.ArgumentParser
    """
    parser = ArgumentParser(prog="datakiln", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True,
                                     metavar="command")

    for name, module, summary, log_file in COMMANDS:
        command = commands.add_parser(name, help=summary, description=summary)
        module.add_arguments(command)
        command.set_defaults(run=module.run, log_file=log_file)

    extract = commands.add_parser(
        "extract", help="Extract data from a source into the dataset.",
        description="Extract data from a source into the dataset.")
    sources = extract.add_subparsers(dest="source", required=True,
                                     metavar="source")
    for name, module, summary in SOURCES:
        source = sources.add_parser(name, help=summary, description=summary)
        module.add_arguments(source)
        source.set_defaults(run=module.run, log_file="extract.log")

    return parser


def main(args: Optional[List[str]] = None):
    """Runs the command that was asked for.

    :param args: Arguments of the command line. Those of the process by
                 default.
    :type args: list[str]
    """
    args = get_parser().parse_args(args)
    if args.log_file:
        setup_logging(args.stdout, args.log_file)
    args.run(args)
