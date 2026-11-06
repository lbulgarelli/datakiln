[![PyPI](https://img.shields.io/pypi/v/datakiln)](https://pypi.org/project/datakiln/)
[![Lint](https://github.com/lbulgarelli/datakiln/actions/workflows/lint.yml/badge.svg)](https://github.com/lbulgarelli/datakiln/actions/workflows/lint.yml)
[![Test](https://github.com/lbulgarelli/datakiln/actions/workflows/test.yml/badge.svg)](https://github.com/lbulgarelli/datakiln/actions/workflows/test.yml)
[![Coverage](https://img.shields.io/endpoint?url=https%3A%2F%2Flbulgarelli.github.io%2Fdatakiln%2Fcoverage.json)](https://lbulgarelli.github.io/datakiln/coverage/)
[![Docs](https://img.shields.io/badge/docs-latest-blue)](https://lbulgarelli.github.io/datakiln/)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/github/license/lbulgarelli/datakiln)](https://github.com/lbulgarelli/datakiln/blob/main/LICENSE)
[![Release](https://img.shields.io/github/v/release/lbulgarelli/datakiln)](https://github.com/lbulgarelli/datakiln/releases)

# datakiln

datakiln builds a database from a repository of files, so that any
version of a dataset can be rebuilt later with one command.

Research datasets can span dozens of tables, with views and
functions that people build on top of them. Each new release of the data, each
fix to a view and each change to a column type leaves a slightly different
database behind, and months later, when someone has to reproduce a result,
nothing in the database says which version of which file it came from.

Here the data files are versioned, and the metadata keeps track of the source
tables, their columns and their constraints. Together with the scripts for the
views and functions, that describes the whole database, so a version of the
dataset is a version of the repository. Because it remembers what it built last
time, moving to another version only reloads what changed and reruns only what
depends on it.

## Why use it

- To get the dataset exactly as it was when a paper was written, go back to
  that version of the repository and build again.
- A change to a column type, a key or a view is recorded like any change to
  code, so others can review it and it can be undone.
- A data pipeline is a chain of steps that depend on one another, and those
  dependencies usually live in someone's head or in conventions that drift.
  Here they are inferred from the repository's own files, so every step runs
  after what it depends on and a change reaches everything built on it.
- How the tables and views depend on each other can be drawn from the
  repository itself, so whoever picks up a dataset they did not build can see
  how it fits together, and the picture cannot drift from the code.

## Features

- **Tables from files.** Files of several types are loaded into database
  tables as their metadata describes: types, required columns, primary,
  unique and foreign keys, indexes, values read as `NULL`, and columns
  unpivoted from wide data.
- **Incremental builds.** A table is rebuilt when its metadata or its data
  file changes, or when a table it references is rebuilt. A script that
  builds views or functions runs again when its content changes or what it
  uses is rebuilt.
- **DVC data files.** The data files can be tracked by DVC, and the ones a
  build needs are pulled from the remote.
- **Views and functions in dependency order.** Each SQL script creates one
  object (a view, function, type...) from the tables or from other objects.
  The tools read the SQL to find what each script uses, so the scripts need no
  numbering, and the order comes from the dependencies.
- **Dependency graph.** The graph of the tables and scripts can be drawn as a
  Mermaid flowchart.
- **Metadata generation.** Table metadata can be generated from sample data
  files, from a source database in one of several DBMS, or from the forms of a
  REDCap project.
- **Extraction from a database.** Data can be extracted from a source database
  for the group of people a study follows (its cohort). A table is limited to
  the rows that relate to the cohort or to another extracted table, directly
  or through a chain of joins, and the tables are extracted in the order that
  those relations require.
- **Pruning and permissions.** Objects removed from the repository are listed
  or dropped, and read access is granted to a database role and its users.

## Installation

It needs Python and a database server. The metadata gives a column type for
each DBMS, so the tool is meant to work with more than one.

```bash
pip install datakiln
```

This installs one command, `datakiln`, with the subcommands `build`,
`metadata`, `graph` and `extract` (`extract dbms` and `extract redcap`). Run
`datakiln --help`, or `--help` after a subcommand, to see all of its options.

## Dataset layout

Every command takes the directory of the dataset:

```
dataset/
├── .env             # Database connection
├── .dataignore      # Files to skip (optional)
├── data/            # Data files, one for each table
├── metadata/        # Table metadata, one JSON file for each table
├── transform/       # SQL scripts that run on the loaded data
├── derive/          # SQL scripts that create views, functions...
└── extract/         # Extraction metadata (only to extract from a DBMS)
```

`.dataignore` lists files, one for each line, relative to the dataset
directory (`derive/old.sql`). They are left out of the build.

### Environment

The connection to the database is set in the `.env` file of the dataset
directory. All of the variables are optional and have the defaults below, but
the file itself must exist to build the database.

| Variable | Default | |
| --- | --- | --- |
| `DB_USER` | the current user | |
| `DB_HOST` | `localhost` | |
| `DB_PORT` | `5432` | |
| `DB_NAME` | `postgres` | Created if it does not exist. |
| `DB_DIALECT` | `postgresql` | The DBMS of the database to build. |
| `DB_ISOLATION_LEVEL` | `READ COMMITTED` | |

```bash
DB_USER=analyst
DB_HOST=db.example.org
DB_NAME=study
```

There is no variable for the database password. The connection uses the
password mechanism of the DBMS, for example:

- PostgreSQL: a `~/.pgpass` file or the `PGPASSWORD` environment variable.

`DB_NAME` also names the read role the build creates (see
[Permissions](#permissions)).

### Data files and DVC

Each table reads the file in `data/` that has its name, ignoring the case and
the suffixes (`.csv`, `.csv.gz`, `.xlsx`). The table of `metadata/patient.json`
reads `data/patient.csv`.

If `data/` is a DVC repository (it has a `data/.dvc` directory), the files are
the ones DVC tracks. A build pulls the files of the tables it builds from the
DVC remote, and tells a changed file by its checksum. Otherwise `data/` is
read as a plain folder and nothing is pulled.

```bash
cd dataset/data
dvc init --subdir
dvc remote add -d storage ssh://user@example.org/path/to/storage
dvc add patient.csv
```

The DVC remote of a pipeline that runs without a terminal needs credentials
that do not ask for a password. For an SSH remote, that is a private key in
`~/.ssh`, or one named with `dvc remote modify --local <remote> keyfile <path>`.

## Usage

### Generate the metadata of the tables

From sample data files, it reads the first 10,000 rows of each file in `data/`
and writes a JSON file for each table to `metadata/`:

```bash
datakiln metadata dataset
datakiln metadata dataset --delimiter ";"
```

The columns are named from the headers, the types are guessed from the
data, and the primary key is a `meta_row_id` column. The schema is `public`.
Edit each file after: set the schema, the key, the real types and the
descriptions.

From a REDCap project, it downloads the data of each form to `data/` and, with
`--metadata`, writes its metadata:

```bash
datakiln extract redcap dataset --api-url https://redcap.example.org/api/ \
    --api-key "$REDCAP_TOKEN" --metadata --schema redcap \
    --include demographics labs --exclude consent
```

From a source database, see [Extract from a database](#extract-from-a-database).

### Build the database

```bash
datakiln build dataset
```

This does the following:

1. Creates the database if it does not exist, and the schema that records what
   was built (the checksums of the tables and scripts).
2. Loads the data of each table that is new or changed, or that references a
   table that changed, into a staging schema (`_<schema>`), and runs the
   `transform/` scripts on it.
3. Moves the rows that satisfy the constraints of each table to its final
   table, which has its types, keys and indexes. The rows that do not (a
   required value is missing, a foreign key has no match or a key is
   duplicated) stay in the staging table, with the reason in its `meta_log`
   column, and a warning gives their count.
4. Runs the `derive/` scripts that changed, each one after the scripts that
   create what it uses.
5. Lists what was built before and is no longer in the repository, and grants
   read access.

| Option | |
| --- | --- |
| `--include name...` | Build only these tables and scripts, and what they use. |
| `--rebuild-all` | Rebuild everything, even what is up to date. |
| `--prune` | Drop what was removed from the repository. Without it, the objects are only listed. |
| `--grant user...` | Give read access to these database users. |
| `--stdout` | Show the log in the terminal. It is always written to `build.log`. |

```bash
# Only one view, with the tables and scripts it needs
datakiln build dataset --include study_derived.labs

# Everything again, and remove what is gone from the repository
datakiln build dataset --rebuild-all --prune
```

A build stops with an error if what is in the repository needs an object that
was removed from it.

### Go back to an earlier version

```bash
git checkout <commit>
datakiln build dataset
```

The build compares the checksums of the files and metadata at that commit with
what is in the database, reloads the tables that differ, and reruns the
scripts built on them. Add `--prune` to drop the objects that did not exist
yet at that commit.

### Table metadata

A table is described by a JSON file in `metadata/`:

```json
{
    "schema": "study",
    "name": "visit",
    "encoding": "utf-8",
    "delimiter": ",",
    "na_values": ["N/A", "unknown"],
    "columns": [
        {
            "name": "Visit ID",
            "rename": "visit_id",
            "description": "Identifier of the visit.",
            "dtype": {"postgresql": "INTEGER"},
            "required": true
        },
        {
            "name": "Patient ID",
            "rename": "patient_id",
            "dtype": {"postgresql": "INTEGER"},
            "required": true
        },
        {
            "name": "Visit date",
            "rename": "visit_date",
            "dtype": {"postgresql": "DATE"}
        }
    ],
    "constraints": {
        "primary_key": ["visit_id"],
        "uniques": [["patient_id", "visit_date"]],
        "foreign_keys": [
            {
                "columns": ["patient_id"],
                "reference": {
                    "schema": "study",
                    "table": "patient",
                    "columns": ["patient_id"]
                }
            }
        ]
    }
}
```

- `name` is the header of the column in the data file, and `rename` is its
  name in the database.
- `dtype` has the type for each dialect.
- A column with `"transform": true` is not in the data file. A `transform/`
  script fills it.
- A foreign key makes the table depend on the one it references, which
  therefore is built first.
- Tables can also have `indexes` and `unpivot` (to turn columns into rows).
  The metadata generated by the commands above, and the code of
  [`TableMeta`](https://github.com/lbulgarelli/datakiln/blob/main/datakiln/metadata/table.py), show their format.

### Transform scripts

The `.sql` files in `transform/` run, in the order of their names, after the
data is loaded into the staging tables and before it moves to the final ones.
They are the place to clean values or to fill the columns marked `transform`.

### Derive scripts

The `.sql` files in `derive/` (in any subfolder) create the objects built from
the tables: views, materialized views, tables, indexes, functions,
procedures, aggregates, types, sequences and schemas.

```sql
-- derive/labs.sql
CREATE MATERIALIZED VIEW study_derived.labs AS
SELECT v.patient_id, v.visit_date, l.test, l.value
FROM study.visit AS v
JOIN study.lab AS l USING (visit_id);
```

The tools read each script to find the objects it uses, so the scripts do not
need numbered names. The rules are:

- A script creates exactly one object, and nothing else.
- Tables and views are in a schema that is not `public`.
- Functions, procedures and aggregates are created and called without a
  schema, so they live in the default schema of the target database. They are
  told apart by the types of their parameters, so overloads work.
- A script that creates a schema (`CREATE SCHEMA study_derived`) is required
  by every script that creates something in it.
- A query given as text is read too, as the ones a `crosstab` takes, and so
  are the strings in the body of a function. They follow the same rules as the
  code around them.
- A script whose SQL cannot be parsed, so its dependencies cannot be worked
  out, is an error that names the file.

### Dependency graph

```bash
datakiln graph dataset                       # Mermaid text
datakiln graph dataset --output graph.md     # Markdown with a Mermaid diagram
datakiln graph dataset --output graph.html   # A page that any browser draws
datakiln graph dataset --direction TB --include study_derived.labs
```

It draws the tables and the scripts as a diagram, with an arrow from what is
needed to what needs it. The extension of the output file tells the format,
and `--include` limits the graph to some objects and what they need.

### Extract from a database

`datakiln extract dbms` downloads data from a source database to `data/`, and can write the table metadata from the source's columns. Each
table is described by a JSON file in `extract/`:

```json
{
    "schema": "source",
    "table": "visit",
    "filters": [
        {
            "columns": ["patient_id"],
            "reference": {
                "schema": "study",
                "table": "cohort",
                "columns": ["patient_id"]
            }
        }
    ]
}
```

A filter limits the rows to the ones that match a reference table, which is
the cohort or another extracted table. Chains of `join`, comparison
`operators`, cohorts given by a `.sql` file and the `chunk_size` of the
download are also available. The format is documented in
[`datakiln/extraction/models.py`](https://github.com/lbulgarelli/datakiln/blob/main/datakiln/extraction/models.py).

```bash
datakiln extract dbms dataset --dialect oracle --host source.example.org --port 1521 \
    --database ORCL --user analyst --metadata
```

| Option | |
| --- | --- |
| `--dialect`, `--host`, `--port`, `--database` | The source DBMS, all required. |
| `--user`, `--password` | The default user is the current one. |
| `--metadata` | Also write the JSON metadata of the tables to `metadata/`. |
| `--no-data` | Only write the metadata, without downloading data. |
| `--include table...` | Extract only these tables, and what they need. |
| `--read-only` | Stream the data, without creating temporary tables in the source. |
| `--parallel N` | Number of extractions at once. Default is 4. |

The metadata is written in the dialect of the target database, set in the
`.env` file of the dataset. `extract redcap` takes the same `--data` and
`--metadata` options, and `--include` and `--exclude` for forms.

### Permissions

The build creates a database role named after the database (`DB_NAME`). It
gets read access to the schemas of the tables and scripts, which leaves out
the staging schemas and the metadata. `--grant` gives that role to users:

```bash
datakiln build dataset --grant alice bob
```

### Logs

`build`, `metadata` and `extract` write a log to the directory they run in:
`build.log`, `metadata.log` or `extract.log` (for both sources). The log is
overwritten on each run, and `--stdout` also prints it. `graph` writes no log.

## Development

The unit tests need a database server that the user in `.env` can use, for
example:

```bash
DB_USER=test
DB_HOST=localhost
DB_NAME=test
```

To work on the code, install it from a clone with the extras for the tests,
the lint and the documentation:

```bash
pip install -e ".[test,lint,docs]"
pytest tests
flake8 datakiln tests
pylint datakiln
```

The documentation of the code is at <https://lbulgarelli.github.io/datakiln/>.

A dataset repository can test its own metadata and scripts by running the
tests in `datakiln/test.py` from its directory. They create a `__test`
database, load the tables, run the transform and `derive/` scripts, and roll
everything back.

```bash
pytest --pyargs datakiln.test
```

## License

MIT. See [LICENSE](https://github.com/lbulgarelli/datakiln/blob/main/LICENSE).
