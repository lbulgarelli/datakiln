"""
Creates table and loads data.
"""
import json
from pathlib import Path
from getpass import getuser
from textwrap import dedent

from sqlalchemy.orm.session import Session
from sqlalchemy import create_engine, text
from decouple import Config, RepositoryEnv, config

from data_utils.build_json import get_parser


class ColumnMeta():
    """Metadata for column."""
    def __init__(self, name, dtype, dialect, rename=None, stage=False):
        """
        :param name: Name of the column.
        :type name: str

        :param dtype: Type of the column for each dialect.
        :type dtype: dict

        :param dialect: Dialect of the database.
        :type dialect: str

        :param rename: Name to be used in the database.
        :type rename: str

        :param stage: Is it in a stage table?
        :type stage: bool

        :return: Column metadata.
        :rtype: data_utils.build_table.ColumnMeta
        """
        self.name = name
        self.dtype = dtype[dialect]
        self.rename = rename or name

        # Do not inforce types in stage
        if stage is True:
            self.dtype = "TEXT"

    def create(self):
        """Column SQL for create statement.

        :return: Column SQL for create statement.
        :rtype: str
        """
        return f"{self.rename} {self.dtype}"


class TableMeta():
    """Metadata for table"""
    ROW_ID = "meta_row_id"

    def __init__(self, name, columns, constraints, dialect, stage=False):
        """
        :param name: Name of the table.
        :type name: str

        :param columns: Metadata for table columns.
        :type columns: list[dict]

        :param constraints: Metadata for table constraints.
        :type constraints: dict

        :param dialect: Dialect of the database.
        :type dialect: str

        :param stage: Is it a stage table?
        :type stage: bool

        :return: Table metadata.
        :rtype: data_utils.build_table.TableMeta
        """
        self.name = name
        self.columns = []
        self.primary_key = constraints["primary_key"]

        # Instantiate metadata object for all columns
        for column in columns:
            self.columns.append(
                ColumnMeta(**column, stage=stage, dialect=dialect))

        # Add autoincreasing rowid
        self.columns.append(ColumnMeta(
            self.ROW_ID, {dialect: "SERIAL"}, dialect))

        # Do not enforce constraints in stage
        if stage is True:
            self.primary_key = [self.ROW_ID]

    def create(self, conn):
        """Create statement.

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine
        """
        stmt = dedent("""
        CREATE TABLE {name} (
            {columns}
          , PRIMARY KEY({pk})
        )
        """)

        stmt = stmt.format(
            name=self.name,
            columns='\n  , '.join(map(ColumnMeta.create, self.columns)),
            pk=', '.join(self.primary_key)
        )

        conn.execute(text(stmt))


def load_env(directory):
    """Load environment variables.

    :param directory: Path for metadata directory.
    :type directory: str
    """
    db_params = {}

    # Environment file directory
    dotenv_file = Path(directory) / ".env"

    # If the file does not exist,
    # try to retrieve from the shell environment
    if dotenv_file.exists():
        env_config = Config(RepositoryEnv(dotenv_file))
    else:
        env_config = config

    # Get environment variables
    db_params["user"] = env_config("DB_USER", getuser())
    db_params["host"] = env_config("DB_HOST", "localhost")
    db_params["port"] = env_config("DB_PORT", "5432")
    db_params["database"] = env_config("DB_NAME", "postgres")
    db_params["dialect"] = env_config("DB_DIALECT", "postgresql")

    return db_params


def connect(user, host, port, database, dialect):
    """Connect to database using SQLAlchemy.

    :param user: Name of the database user.
    :type user: str

    :param host: Hostname of the database server.
    :type host: str

    :param port: Network port of the database server.
    :type port: int

    :param database: Name of the database.
    :type database: str

    :param dialect: Dialect of the database.
    :type dialect: str

    :return: SQLAlchemy session.
    :rtype: sqlalchemy.orm.session.Session
    """
    engine = create_engine(
        f"{dialect}://{user}:@{host}:{port}/{database}")
    con = engine.connect()
    return Session(bind=con)


def main(args=None):
    """Creates table and loads data."""
    # Get arguments
    parser = get_parser()
    args = parser.parse_args(args)

    # Get absolute of specified path
    meta_path = Path(args.directory).absolute()

    # Connect to database
    db_params = load_env(meta_path.parents[1])
    conn = connect(**db_params)

    # Load metadata
    with open(meta_path, encoding="utf-8") as file:
        meta_json = json.load(file)
    meta = TableMeta(dialect=db_params["dialect"],
                     stage=True,
                     **meta_json)

    # Create table
    meta.create(conn)
    conn.commit()


if __name__ == "__main__":
    main()
