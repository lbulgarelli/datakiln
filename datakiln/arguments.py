"""
Options that the commands share, and the log they write.
"""
import logging
import sys
from argparse import ArgumentParser, BooleanOptionalAction


def add_directory(parser: ArgumentParser):
    """Adds the directory of the dataset, which every command takes.

    :param parser: Parser of a command.
    :type parser: argparse.ArgumentParser
    """
    parser.add_argument("directory", metavar="directory", type=str,
                        help="Path of the directory of the dataset.")


def add_log(parser: ArgumentParser):
    """Adds the option to show the log, which commands write to a file.

    :param parser: Parser of a command.
    :type parser: argparse.ArgumentParser
    """
    parser.add_argument("--stdout", action="store_true",
                        help="Output logs to standard output as well as to "
                             "the log file.")


def add_include(parser: ArgumentParser, what: str):
    """Adds the option that limits a command to some of the things it works
    on.

    :param parser: Parser of a command.
    :type parser: argparse.ArgumentParser

    :param what: What the command does with the names, as a sentence.
    :type what: str
    """
    parser.add_argument("--include", metavar="name", type=str, nargs="+",
                        help=what)


def add_export(parser: ArgumentParser):
    """Adds the options that tell what a command that reads from a source
    writes to the dataset.

    :param parser: Parser of a command.
    :type parser: argparse.ArgumentParser
    """
    parser.add_argument("--data", action=BooleanOptionalAction, default=True,
                        help="Whether to write the data files.")
    parser.add_argument("--metadata", action=BooleanOptionalAction,
                        default=False,
                        help="Whether to write the metadata files of the "
                             "tables.")


def setup_logging(stdout: bool, log_file: str):
    """Sets up logging for a command.

    :param stdout: Whether to output logs to standard output.
    :type stdout: bool

    :param log_file: Path to the log file.
    :type log_file: str
    """
    handlers = [logging.FileHandler(log_file, mode="w")]
    if stdout:
        handlers.append(logging.StreamHandler(sys.stdout))

    logging.basicConfig(
        level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S",
        format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
        handlers=handlers
    )
