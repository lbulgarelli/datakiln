"""Compose SQL cohorts with the extraction statement's top-level CTEs."""
from dataclasses import dataclass

import sqlglot
from sqlalchemy import column, text
from sqlalchemy.engine import Dialect
from sqlalchemy.sql import quoted_name
from sqlalchemy.sql.selectable import CTE
from sqlglot import exp
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlglot.optimizer.scope import Scope, traverse_scope
from sqlglot.transforms import move_ctes_to_top_level

from datakiln.metadata.dialect import DIALECT_MAP


@dataclass
class SQLCohort:
    """Expose a SQL cohort and its dependencies as SQLAlchemy CTEs.

    :param query: Cohort SELECT, including any existing WITH clauses.
    :type query: str

    :param name: Unique name of this cohort within the extraction statement.
    :type name: str

    :param dialect: Source database dialect.
    :type dialect: sqlalchemy.engine.Dialect
    """

    query: str
    name: str
    dialect: Dialect

    def _namespace(self, expression: exp.Expression) -> None:
        """Give every cohort CTE a unique name without changing its aliases.

        :param expression: Parsed cohort SELECT to update in place.
        :type expression: sqlglot.exp.Expression
        """
        # Resolve names on a normalized copy, preserving the original case
        # of physical tables, output columns and quoted identifiers.
        normalized = expression.copy()
        originals = {}
        for original, node in zip(expression.walk(), normalized.walk()):
            originals[id(node)] = original
        dialect = DIALECT_MAP.get(self.dialect.name, self.dialect.name)
        normalize_identifiers(normalized, dialect=dialect)

        # Oracle and SQL Server allow recursive references without the
        # RECURSIVE keyword. Make that explicit only for scope resolution.
        if self.dialect.name in ('oracle', 'mssql'):
            for with_clause in normalized.find_all(exp.With):
                with_clause.set('recursive', True)

        # Scope-aware names keep shadowed CTEs separate and prevent clashes
        # with CTEs from other cohort files or the extraction query itself.
        names = {}
        for node in normalized.find_all(exp.CTE):
            name = f'{self.name}_{len(names)}'
            names[id(node)] = name
            originals[id(node)].args['alias'].set(
                'this', exp.to_identifier(name)
            )

        # Rename only references resolved to CTEs, not physical tables with
        # the same name. Keep their old aliases so qualifiers remain valid.
        for scope in traverse_scope(normalized):
            for node, source in scope.selected_sources.values():
                if not isinstance(source, Scope) or not source.is_cte:
                    continue
                definition = source.expression.find_ancestor(exp.CTE)
                if definition is None:
                    continue
                original = originals[id(node)]
                if not original.args.get('alias'):
                    original.set('alias', exp.TableAlias(
                        this=original.this.copy()
                    ))
                original.set('this', exp.to_identifier(names[id(definition)]))

    def _definition(self, definition: exp.CTE) -> CTE:
        """Preserve a cohort CTE's column aliases and materialization options.

        :param definition: Namespaced CTE definition from the cohort query.
        :type definition: sqlglot.exp.CTE

        :return: SQLAlchemy CTE ready for the outer WITH clause.
        :rtype: sqlalchemy.sql.selectable.CTE
        """
        # Explicit CTE column lists may rename the query's output columns.
        columns = []
        for identifier in definition.args['alias'].args.get('columns') or []:
            name = quoted_name(identifier.name, identifier.args.get('quoted'))
            columns.append(column(name))

        # A recursive CTE must retain recursive syntax and named outputs.
        recursive = False
        for table in definition.this.find_all(exp.Table):
            if table.name == definition.alias and not table.db:
                recursive = True
                break
        if recursive and not columns:
            for selected in definition.this.selects:
                identifier = selected.args.get('alias')
                if identifier is None and isinstance(selected, exp.Column):
                    identifier = selected.this
                name = selected.output_name
                if identifier is not None:
                    name = quoted_name(name, identifier.args.get('quoted'))
                columns.append(column(name))

        # SQLAlchemy combines these definitions with the extraction's CTEs;
        # the raw SQL body no longer contains a nested WITH clause.
        dialect = DIALECT_MAP.get(self.dialect.name, self.dialect.name)
        statement = text(definition.this.sql(dialect=dialect)).columns(
            *columns
        )
        statement.name_cte_columns = bool(columns)
        result = statement.cte(definition.alias, recursive=recursive)

        # PostgreSQL's explicit materialization choice belongs to the CTE,
        # whereas Oracle optimizer hints remain inside its SELECT body.
        materialized = definition.args.get('materialized')
        if materialized is not None:
            option = 'MATERIALIZED' if materialized else 'NOT MATERIALIZED'
            result = result.prefix_with(option, dialect='postgresql')
        return result

    def build(self) -> CTE:
        """Build one reusable cohort CTE with all dependencies hoisted."""
        # Parse only the cohort SQL, leaving the generated extraction query
        # and its JSON parameters under SQLAlchemy's control.
        dialect = DIALECT_MAP.get(self.dialect.name, self.dialect.name)
        expression = sqlglot.parse_one(self.query, read=dialect)
        self._namespace(expression)
        move_ctes_to_top_level(expression)

        # Remove WITH from the body and register each dependency separately
        # so the final statement emits one top-level WITH in dependency order.
        definitions = []
        with_clause = expression.args.get('with_')
        if with_clause is not None:
            with_clause.pop()
            for definition in with_clause.expressions:
                definitions.append(self._definition(definition))
        statement = text(expression.sql(dialect=dialect)).columns()
        return statement.add_cte(*definitions).cte(self.name)
