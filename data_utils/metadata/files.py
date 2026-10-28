"""
Data files of the tables.
"""
from hashlib import md5
from pathlib import Path
from typing import List

from dvc.api import DVCFileSystem


class LocalFile(dict):
    """Local file or folder, with the keys of one that DVC tracks. Its
    checksum is computed when it is needed, as that takes reading it."""

    def __init__(self, path: Path, repo: 'LocalRepo'):
        super().__init__(name=path.name, repo=repo)
        self._path = path

    def __missing__(self, key):
        if key != "md5":
            raise KeyError(key)

        # A folder is told by the names and the content of its files
        files = [self._path]
        if self._path.is_dir():
            files = sorted(self._path.rglob("*"))

        checksum = md5()  # noqa: DUO130
        for file in files:
            if file.is_dir():
                continue
            if self._path.is_dir():
                checksum.update(str(file.relative_to(self._path)).encode())
            with open(file, "rb") as content:
                while chunk := content.read(4096):
                    checksum.update(chunk)

        self[key] = checksum.hexdigest()
        return self[key]


class LocalRepo:
    """Folder of data files that DVC does not track. It has the part of a DVC
    repository, and of its file system, that the tables use, so that there is
    nothing to fetch."""

    def __init__(self, root_dir: Path):
        self.root_dir = str(root_dir)

    def listdir(self, path: str, dvc_only: bool = False) -> List[LocalFile]:
        # pylint: disable=unused-argument
        """Files and folders, other than the hidden ones and the ones of DVC.

        :param path: Folder in the root.
        :type path: str

        :param dvc_only: Ignored, as every file is local.
        :type dvc_only: bool

        :rtype: list[LocalFile]
        """
        files = []
        for file in sorted((Path(self.root_dir) / path).iterdir()):
            if not file.name.startswith(".") and file.suffix != ".dvc":
                files.append(LocalFile(file, self))
        return files

    def pull(self, targets):
        # pylint: disable=unused-argument
        """Nothing is fetched, since the files are local.

        :return: What was fetched, which is nothing.
        :rtype: dict
        """
        return {}


class DataFiles:
    # pylint: disable=too-few-public-methods
    """Data files of the tables. They are the ones DVC tracks if the folder is
    a DVC repository, and the ones in it otherwise."""

    def __init__(self, directory: Path):
        """
        :param directory: Folder of the data files.
        :type directory: Path
        """
        directory = Path(directory)
        if (directory / ".dvc").is_dir():
            self._files = DVCFileSystem(directory)
        else:
            self._files = LocalRepo(directory)

    @staticmethod
    def _named(path: str, name: str) -> bool:
        """Whether a file is named so, without its suffixes and ignoring the
        case."""
        path = Path(path)
        suffix = "".join(path.suffixes)
        stem = path.name[:-len(suffix)] if suffix else path.name
        return stem.lower() == name

    def find(self, name: str) -> dict:
        """Find the data of a table.

        :param name: Name of the table metadata, in lower case.
        :type name: str

        :return: Name of the file or folder in the folder of the data files,
                 its checksum and the repository it belongs to.
        :rtype: dict

        :raises FileNotFoundError: If there is no file for the table.
        """
        for artifact in self._files.listdir(".", dvc_only=True):
            if self._named(artifact["name"], name):
                return artifact

        raise FileNotFoundError(f"No files found for metadata: '{name}'")
