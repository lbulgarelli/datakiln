"""
Creates table and loads data.
"""
import logging
from pathlib import Path
from textwrap import dedent

from sqlalchemy import text

from data_utils.build_json import get_parser
from data_utils.build_table import load_env, connect, stage


logging.basicConfig(
    level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S",
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s")


def execute_directory(directory, conn):
    """Executes SQL scripts in a given directory.

    :param directory: Path for metadata directory.
    :type directory: str

    :param conn: SQLAlchemy connection or session.
    :type conn: sqlalchemy.Engine
    """
    for file_name in sorted(directory.glob("*.sql")):
        logging.info("Script '%s/%s': RUNNING", directory.stem, file_name.stem)

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

    # Iterate through all table metadata
    tables_meta = []
    for meta_path in sorted((directory / "metadata").glob("*.json")):
        # Stage and flush to database
        tables_meta.append(stage(meta_path, conn))
        conn.flush()

    # Run transforms
    execute_directory(directory / "transform", conn)

    # Load to final schema and flush to database
    for table_meta in tables_meta:
        table_meta.enforce_constraints(conn)
        table_meta.load(conn)

    # Run views
    execute_directory(directory / "derive", conn)

    # Grant read permissions on entire database
    grant(conn, db_params["database"], args.grant)

    # Commit changes
    conn.commit()


if __name__ == "__main__":
    main()
