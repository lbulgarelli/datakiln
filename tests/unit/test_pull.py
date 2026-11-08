"""
Test fetching the data files from a DVC remote, or from the URL they were
imported from
"""

import pytest
from dvc.exceptions import DvcException
from dvc.repo import Repo
from dvc.stage.exceptions import StageFileDoesNotExistError

from datakiln.metadata.table import TableMeta


def make_table(repo):
    """Table of the file data.csv of a DVC repository."""
    table = object.__new__(TableMeta)
    table.name, table.schema = "data", "test"
    table.artifact = {"name": "data.csv", "repo": repo}
    return table


def add_remote(repo, url, default=True):
    """Add a DVC remote to a repository."""
    with repo.config.edit() as conf:
        conf["remote"]["storage"] = {"url": str(url)}
        if default:
            conf["core"]["remote"] = "storage"


def remove_data(tmp_path, repo_dir):
    """Remove the data file and the cache of a repository."""
    (repo_dir / "data.csv").unlink()
    for cached in (repo_dir / ".dvc" / "cache").rglob("*"):
        if cached.is_file():
            cached.rename(tmp_path / f"cache-{cached.name}")


@pytest.fixture(name="repo_dir")
def fixture_repo_dir(tmp_path):
    """Folder of a DVC repository, with a file at a URL."""
    (tmp_path / "upstream").mkdir()
    (tmp_path / "upstream" / "data.csv").write_text("a,b\n1,2\n")
    (tmp_path / "repo").mkdir()
    return tmp_path / "repo"


@pytest.fixture(name="imported")
def fixture_imported(tmp_path, repo_dir):
    """Table whose file was imported from the URL, and removed."""
    repo = Repo.init(str(repo_dir), no_scm=True)
    repo.imp_url(str(tmp_path / "upstream" / "data.csv"),
                 str(repo_dir / "data.csv"))
    remove_data(tmp_path, repo_dir)
    return make_table(repo)


def test_pull_present(imported, repo_dir):
    """A file that is there is left as it is"""
    (repo_dir / "data.csv").write_text("local")
    imported.pull()
    assert (repo_dir / "data.csv").read_text() == "local"


def test_pull_remote(tmp_path, repo_dir):
    """The file is pulled from the default remote"""
    repo = Repo.init(str(repo_dir), no_scm=True)
    add_remote(repo, tmp_path / "storage")
    (repo_dir / "data.csv").write_text("a,b\n1,2\n")
    repo.add(str(repo_dir / "data.csv"))
    repo.push()
    remove_data(tmp_path, repo_dir)

    make_table(repo).pull()

    assert (repo_dir / "data.csv").read_text() == "a,b\n1,2\n"


def test_pull_download(imported, repo_dir):
    """Without a remote, the file is downloaded from the URL, and the DVC
    file is kept"""
    recorded = (repo_dir / "data.csv.dvc").read_text()

    imported.pull()

    assert (repo_dir / "data.csv").read_text() == "a,b\n1,2\n"
    assert (repo_dir / "data.csv.dvc").read_text() == recorded


def test_pull_remote_error(tmp_path, imported):
    """A default remote that does not have the file is an error, even if the
    file was imported"""
    add_remote(imported.artifact["repo"], tmp_path / "empty")

    with pytest.raises(DvcException):
        imported.pull()


def test_pull_other_remote(tmp_path, imported, repo_dir):
    """A remote that is not the default does not stop the download"""
    add_remote(imported.artifact["repo"], tmp_path / "empty", default=False)

    imported.pull()

    assert (repo_dir / "data.csv").read_text() == "a,b\n1,2\n"


def test_pull_changed(tmp_path, imported, repo_dir):
    """A changed file at the URL is not used, and the DVC file is kept"""
    recorded = (repo_dir / "data.csv.dvc").read_text()
    (tmp_path / "upstream" / "data.csv").write_text("a,b\n9,9\n")

    with pytest.raises(ValueError, match="changed"):
        imported.pull()

    assert not (repo_dir / "data.csv").exists()
    assert (repo_dir / "data.csv.dvc").read_text() == recorded


def test_pull_nowhere(repo_dir):
    """Without a remote or an import, there is nowhere to get it from"""
    repo = Repo.init(str(repo_dir), no_scm=True)

    with pytest.raises(StageFileDoesNotExistError):
        make_table(repo).pull()
