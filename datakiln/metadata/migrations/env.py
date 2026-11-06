"""
Environment of the migrations, which run on the connection they are given.
"""
# pylint: disable=no-member
from alembic import context

from datakiln.metadata.table import Base


context.configure(
    connection=context.config.attributes["connection"],
    target_metadata=Base.metadata,
    version_table_schema=Base.metadata.schema,
)

with context.begin_transaction():
    context.run_migrations()
