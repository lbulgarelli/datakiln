# Contributing to datakiln

Thank you for taking the time. Bug reports, questions and changes are all
welcome.

## Reporting a problem or asking for something

Open an [issue](https://github.com/lbulgarelli/datakiln/issues). For a bug,
say what you ran (the `datakiln` command and its options), what you expected,
what happened instead, and the version (`pip show datakiln`), the Python
version and the DBMS. The log of the command (`build.log`, `metadata.log` or
`extract.log`) helps, with anything private removed. Please do not post
credentials or patient data.

For a security problem, see [SECURITY.md](SECURITY.md) and do not open a
public issue.

## Working on the code

```bash
git clone https://github.com/lbulgarelli/datakiln.git
cd datakiln
pip install -e ".[test,lint,docs]"
```

The tests build tables in a database, so they need a PostgreSQL server that
you can create databases on, and a `.env` file in the directory you run them
from:

```bash
DB_USER=postgres
DB_HOST=localhost
DB_NAME=postgres
```

Run what the checks of the repository run:

```bash
pytest tests/unit
pylint datakiln
flake8 datakiln tests
flake8 --select=DUO datakiln
```

The checks run on Python 3.10 to 3.13 for the tests, and on every pull
request.

## The code and the changes

- Lines are at most 79 characters, and docstrings use reST fields (`:param`,
  `:type`, `:return:`, `:rtype:`).
- Keep a change to one concern. The first line of the message of a commit
  says in plain words what changed, and the body says why it was needed and
  what behavior is different.
- A change in behavior comes with a test, or with the reason there is none.
- By contributing, you agree that your contribution is released under the
  [MIT license](LICENSE) of the project.

Open a pull request against `main` and describe what it changes. If you are
not sure that a change fits, open an issue first.
