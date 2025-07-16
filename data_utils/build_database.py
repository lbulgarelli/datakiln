"""
Creates table and loads data.
"""
import logging
from pathlib import Path
from textwrap import dedent

from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

from data_utils.build_metadata import get_parser
from data_utils.build_table import stage
from data_utils.database import load_env, connect


logging.basicConfig(
    level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S",
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s")


def execute_directory(directory, conn, ignore):
    """Executes SQL scripts in a given directory.

    :param directory: Path for metadata directory.
    :type directory: str

    :param conn: SQLAlchemy connection or session.
    :type conn: sqlalchemy.Engine

    :param ignore: List of files to ignore.
    :type ignore: str
    """
    for file_name in sorted(directory.glob("**/*.sql")):
        file_relative = file_name.relative_to(directory.parent)
        if str(file_relative) in ignore:
            continue
        logging.info("Script '%s': RUNNING",
                     file_name.relative_to(directory.parent))

        # Load file
        with open(file_name, "r", encoding="utf-8") as file:
            sql = text(file.read())

        # Execute query
        conn.execute(sql)

        # Flush to database
        conn.flush()


def grant(conn, role, users=None):
    """Grants read on all objects in database.

    :param conn: SQLAlchemy connection or session.
    :type conn: sqlalchemy.Engine

    :param role: Name of the role to grant permissions to.
    :type role: str
    """
    # Iterates all schemas in database and grant
    # read permissions to database role
    conn.execute(text(dedent(f"""
    DO $do$
    DECLARE
      schema TEXT;
    BEGIN
      FOR schema IN (
        SELECT nspname
        FROM pg_namespace
        WHERE nspname NOT LIKE 'information_schema%'
        AND nspname NOT LIKE 'pg_catalog%'
        AND nspname NOT LIKE 'pg_toast%'
        AND nspname NOT LIKE 'pg_temp%'
      ) LOOP
        EXECUTE format($$ GRANT USAGE ON SCHEMA %I TO {role} $$, schema);
        EXECUTE format($$ GRANT SELECT ON ALL SEQUENCES IN SCHEMA %I TO {role} $$, schema);
        EXECUTE format($$ GRANT SELECT ON ALL TABLES IN SCHEMA %I TO {role} $$, schema);
      END LOOP;
    END;
    $do$;""")))  # noqa: E501

    # Grant permissions to users by proxy of the database role
    if users:
        conn.execute(text(dedent("""
        GRANT {role} TO "{users}"
        """).format(
            role=role,
            users='", "'.join(users))
        ))


def create_role(conn, database):
    """Connect to database using SQLAlchemy.

    :param conn: SQLAlchemy connection or session.
    :type conn: sqlalchemy.Engine

    :param database: Name of the database.
    :type database: str
    """
    try:
        conn.execute(text(dedent("""
        CREATE ROLE {database}
        """).format(database=database)))
    except ProgrammingError as error:
        message = f'role "{database}" already exists'
        if message in error._message():  # pylint: disable=protected-access
            return
        raise error


def main(args=None):
    """Creates table and loads data."""
    # Get arguments
    parser = get_parser()
    parser.add_argument("--grant", metavar="grant", type=str, nargs='+',
                        help="Grant read permissions to specified users")
    args = parser.parse_args(args)

    # Get absolute of specified path
    directory = Path(args.directory).absolute()

    # Connect to database
    db_params = load_env(directory)
    conn = connect(**db_params)

    # Create role for database read access
    create_role(conn, db_params["database"])
    conn.commit()

    # Retrieve files to ignore
    ignore = []
    if (ignore_file := directory / ".dataignore").exists():
        with ignore_file.open("r", encoding="utf-8", errors="ignore") as file:
            ignore = file.read().splitlines()

    # Iterate through all table metadata
    tables_meta = []
    for meta_path in sorted((directory / "metadata").glob("*.json")):
        meta_relative = meta_path.relative_to(directory)
        if str(meta_relative) in ignore:
            continue

        # Stage and flush to database
        tables_meta.append(stage(meta_path, conn))
        conn.flush()

    # Run transforms
    execute_directory(directory / "transform", conn, ignore)

    # Load to final schema and flush to database
    for table_meta in tables_meta:
        table_meta.enforce_constraints(conn)
        table_meta.load(conn)
        table_meta.create_indexes(conn)
        conn.flush()

    # Run views
    execute_directory(directory / "derive", conn, ignore)

    # Grant read permissions on entire database
    grant(conn, db_params["database"], args.grant)

    # Commit changes
    conn.commit()


if __name__ == "__main__":
    main()
