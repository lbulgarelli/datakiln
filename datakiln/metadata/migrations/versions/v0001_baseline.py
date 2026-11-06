"""Tables and artifacts, keyed by schema and table

The metadata of the databases built before there were migrations.

Revision ID: 0001
Revises:
"""
# pylint: disable=no-member, invalid-name, duplicate-code
from alembic import op
import sqlalchemy as sa


revision = "0001"
down_revision = None

SCHEMA = "__metadata"


def upgrade():
    """Create the tables of the first version."""
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


def downgrade():
    """Drop the tables of the first version."""
    op.drop_table("artifact_", schema=SCHEMA)
    op.drop_table("table_", schema=SCHEMA)
