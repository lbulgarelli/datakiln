"""
Environment files of a dataset, which hold the parameters that the commands
would take on the command line, such as passwords.
"""
from pathlib import Path

from decouple import Config, RepositoryEmpty, RepositoryEnv

# File that sets the database that `build` writes to
BUILD_ENV = ".build.env"

# File that sets the source that `extract` reads from
EXTRACT_ENV = ".extract.env"


def load_env_file(directory, names: tuple[str, ...],
                  required: bool = False) -> Config:
    """Reads the first of the files that exists in the directory.

    :param directory: Directory of the dataset.
    :type directory: str

    :param names: File names to look for, in order.
    :type names: tuple[str, ...]

    :param required: Whether one of the files must exist. If not, the
                     environment variables and the defaults are used when
                     none does.
    :type required: bool

    :return: Values of the file, or of the environment when it has none.
    :rtype: decouple.Config

    :raises FileNotFoundError: If the file is required and none exists.
    """
    for name in names:
        file = Path(directory) / name
        if file.exists():
            return Config(RepositoryEnv(file))

    if required:
        raise FileNotFoundError(
            f"No {' or '.join(names)} file in '{directory}'.")

    return Config(RepositoryEmpty())
