"""Tables and scripts as objects of one kind

The tables are recorded by their full name in a table of objects, the same as
the scripts, with the rest of what is specific to a kind in its own table.

Revision ID: 0002
Revises: 0001
"""
# pylint: disable=no-member, invalid-name, duplicate-code
from alembic import op
import sqlalchemy as sa


revision = "0002"
down_revision = "0001"

SCHEMA = "__metadata"


def upgrade():
    """Create the tables of objects, and move the rows of the old ones."""
    op.rename_table("artifact_", "artifact_old_", schema=SCHEMA)
    op.rename_table("table_", "table_old_", schema=SCHEMA)

    op.create_table(
        "object_",
        sa.Column("name_", sa.String(), nullable=False),
        sa.Column("kind_", sa.String(), nullable=False),
        sa.Column("checksum_", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("name_"),
        schema=SCHEMA,
    )
    op.create_table(
        "table_",
        sa.Column("name_", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("name_"),
        sa.ForeignKeyConstraint(
            ["name_"], [f"{SCHEMA}.object_.name_"], ondelete="CASCADE"),
        schema=SCHEMA,
    )
    op.create_table(
        "script_",
        sa.Column("name_", sa.String(), nullable=False),
        sa.Column("drop_", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("name_"),
        sa.ForeignKeyConstraint(
            ["name_"], [f"{SCHEMA}.object_.name_"], ondelete="CASCADE"),
        schema=SCHEMA,
    )
    op.create_table(
        "artifact_",
        sa.Column("path_", sa.String(), nullable=False),
        sa.Column("name_", sa.String(), nullable=False),
        sa.Column("checksum_", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("path_"),
        sa.ForeignKeyConstraint(
            ["name_"], [f"{SCHEMA}.table_.name_"], ondelete="CASCADE"),
        schema=SCHEMA,
    )

    # Move the rows, with the names in lowercase as the database folds them
    table = sa.table(
        "table_old_", sa.column("schema_"), sa.column("table_"),
        sa.column("checksum_"), schema=SCHEMA)
    artifact = sa.table(
        "artifact_old_", sa.column("path_"), sa.column("schema_"),
        sa.column("table_"), sa.column("checksum_"), schema=SCHEMA)
    objects = sa.table(
        "object_", sa.column("name_"), sa.column("kind_"),
        sa.column("checksum_"), schema=SCHEMA)
    tables = sa.table("table_", sa.column("name_"), schema=SCHEMA)
    artifacts = sa.table(
        "artifact_", sa.column("path_"), sa.column("name_"),
        sa.column("checksum_"), schema=SCHEMA)

    name = sa.func.lower(table.c.schema_ + "." + table.c.table_)
    op.execute(sa.insert(objects).from_select(
        ["name_", "kind_", "checksum_"],
        sa.select(name, sa.literal("table"), table.c.checksum_)))
    op.execute(sa.insert(tables).from_select(["name_"], sa.select(name)))

    name = sa.func.lower(artifact.c.schema_ + "." + artifact.c.table_)
    op.execute(sa.insert(artifacts).from_select(
        ["path_", "name_", "checksum_"],
        sa.select(artifact.c.path_, name, artifact.c.checksum_)))

    op.drop_table("artifact_old_", schema=SCHEMA)
    op.drop_table("table_old_", schema=SCHEMA)


def downgrade():
    """Recreate the tables of the first version from the objects.

    The names come back in lowercase, as the database folds them, and the
    scripts are dropped, since the first version did not record them.
    """
    objects = sa.table(
        "object_", sa.column("name_"), sa.column("checksum_"), schema=SCHEMA)
    tables = sa.table("table_", sa.column("name_"), schema=SCHEMA)
    artifacts = sa.table(
        "artifact_", sa.column("path_"), sa.column("name_"),
        sa.column("checksum_"), schema=SCHEMA)

    # Keep the rows, since the tables have the same names in both versions
    bind = op.get_bind()
    saved_tables = bind.execute(
        sa.select(objects.c.name_, objects.c.checksum_).select_from(
            objects.join(tables, objects.c.name_ == tables.c.name_))
    ).all()
    saved_artifacts = bind.execute(sa.select(
        artifacts.c.path_, artifacts.c.name_, artifacts.c.checksum_)).all()

    op.drop_table("artifact_", schema=SCHEMA)
    op.drop_table("table_", schema=SCHEMA)
    op.drop_table("script_", schema=SCHEMA)
    op.drop_table("object_", schema=SCHEMA)

    op.create_table(
        "table_",
        sa.Column("schema_", sa.String(), nullable=False),
        sa.Column("table_", sa.String(), nullable=False),
        sa.Column("checksum_", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("schema_", "table_"),
        schema=SCHEMA,
    )
    op.create_table(
        "artifact_",
        sa.Column("path_", sa.String(), nullable=False),
        sa.Column("schema_", sa.String(), nullable=False),
        sa.Column("table_", sa.String(), nullable=False),
        sa.Column("checksum_", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("path_"),
        sa.ForeignKeyConstraint(
            ["table_", "schema_"],
            [f"{SCHEMA}.table_.table_", f"{SCHEMA}.table_.schema_"],
        ),
        schema=SCHEMA,
    )

    # A full name is the schema and the table
    old_tables = []
    for name, checksum in saved_tables:
        schema, _, table = name.partition(".")
        old_tables.append(
            {"schema_": schema, "table_": table, "checksum_": checksum})

    old_artifacts = []
    for path, name, checksum in saved_artifacts:
        schema, _, table = name.partition(".")
        old_artifacts.append({
            "path_": path, "schema_": schema, "table_": table,
            "checksum_": checksum
        })

    for name, rows in (("table_", old_tables), ("artifact_", old_artifacts)):
        if rows:
            op.bulk_insert(
                sa.table(name, *map(sa.column, rows[0]), schema=SCHEMA), rows)
