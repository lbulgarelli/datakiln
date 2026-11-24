"""Measure read-only statements without storing their rows or bind values."""
import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from time import monotonic
from typing import Any

from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError, SQLAlchemyError
from sqlalchemy.sql import Select
from sqlalchemy.sql.selectable import CompoundSelect

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProbeOptions:
    """Limits and output destination of the filter comparisons."""

    output: Path
    seed_counts: tuple[int, ...] = (100, 1000, 10000, 0)
    chunk_sizes: tuple[int, ...] = (10000, 50000, 0)
    max_rows: int = 10000
    timeout: int = 60
    repeats: int = 1
    explain: bool = False


def error_fields(error: SQLAlchemyError) -> dict[str, Any]:
    """Keep driver codes without SQL, parameters or database message text.

    :param error: Wrapped database exception.
    :type error: sqlalchemy.exc.SQLAlchemyError
    :return: Safe error classification for a report.
    :rtype: dict[str, typing.Any]
    """
    original = getattr(error, 'orig', error)
    detail = original.args[0] if original.args else None
    codes = re.findall(r'\b(?:DPY|DPI|ORA)-\d+', str(original))
    full_code = getattr(detail, 'full_code', None)
    return {'status': 'database_error',
            'error_type': type(original).__name__,
            'database_code': getattr(detail, 'code', None),
            'database_full_code': full_code or next(iter(codes), None),
            'database_codes': list(dict.fromkeys(codes))}


def write_record(path: Path, record: dict[str, Any]) -> None:
    """Append one completed event to the JSON lines report.

    :param path: Existing report file.
    :type path: pathlib.Path
    :param record: Aggregate measurements without bind or row values.
    :type record: dict[str, typing.Any]
    """
    # Close after each event so completed probes survive interruption.
    with path.open('a', encoding='utf-8') as report:
        report.write(json.dumps(record) + '\n')


class StatementProbe:  # pylint: disable=too-few-public-methods
    """Run one statement with separate execution and fetching measurements."""

    def __init__(self, engine: Engine, options: ProbeOptions):
        """Keep the engine and limits used by each fresh connection.

        :param engine: Source database engine.
        :type engine: sqlalchemy.engine.Engine
        :param options: Probe limits and report destination.
        :type options: datakiln.extraction.probes.ProbeOptions
        """
        self.engine = engine
        self.options = options

    def run(self, statement: Select | CompoundSelect,
            labels: dict[str, Any]) -> dict[str, Any]:
        """Record a probe, continuing after database errors and timeouts.

        :param statement: Read-only statement to execute.
        :type statement: sqlalchemy.sql.Select | sqlalchemy.sql.CompoundSelect
        :param labels: Table, variant and seed metadata for this probe.
        :type labels: dict[str, typing.Any]
        :return: Completed measurements, also appended to the report.
        :rtype: dict[str, typing.Any]
        """
        # Compile without literal binds; only aggregate lengths are saved.
        compiled = statement.compile(dialect=self.engine.dialect)
        lengths = []
        for value in compiled.params.values():
            lengths.append(len(str(value)))
        record = dict(labels, phase='connect',
                      sql_characters=len(str(compiled)),
                      parameter_count=len(lengths),
                      parameter_characters=sum(lengths),
                      largest_parameter=max(lengths, default=0))
        write_record(self.options.output, dict(record, event='start'))
        logger.info("Probe '%s': Starting %s.", record['table'], labels)

        # Database messages can contain binds or SQL, so retain codes only.
        started = monotonic()
        try:
            with self.engine.connect() as connection:
                driver = connection.connection.driver_connection
                previous = self._set_timeout(driver)
                try:
                    self._execute(connection, statement, record)
                except DBAPIError:
                    # Discard even apparently healthy failed connections;
                    # Oracle call timeouts can leave unusable pooled state.
                    if not connection.invalidated:
                        connection.invalidate()
                    raise
                finally:
                    if previous is not None and not connection.invalidated:
                        driver.call_timeout = previous
        except SQLAlchemyError as error:
            record.update(error_fields(error))
        record['elapsed_seconds'] = monotonic() - started
        write_record(self.options.output, dict(record, event='finish'))
        logger.info("Probe '%s': Finished %s.", record['table'], record)
        return record

    def _set_timeout(self, driver: Any) -> int | None:
        """Bound Oracle round trips and remember the previous setting."""
        # Other dialects are useful for offline tests, but lack this API.
        if self.engine.dialect.name != 'oracle':
            return None
        previous = driver.call_timeout
        driver.call_timeout = self.options.timeout * 1000
        return previous

    def _execute(self, connection: Any, statement: Select | CompoundSelect,
                 record: dict[str, Any]) -> None:
        """Measure execution, the first row and subsequent batches."""
        # Log before blocking so an interrupted report names the operation.
        started = monotonic()
        record['phase'] = 'execute'
        write_record(self.options.output, dict(record, event='phase'))
        try:
            result = connection.execute(statement.execution_options(
                stream_results=True, skip_formatting=True))
        except DBAPIError:
            record['execute_seconds'] = monotonic() - started
            raise
        record['execute_seconds'] = monotonic() - started
        try:
            self._read(result, record, started)
        except DBAPIError:
            # Invalidate before cursor cleanup, which otherwise tries to
            # close a cursor on an already disconnected Oracle connection.
            connection.invalidate()
            raise
        finally:
            if not connection.invalidated:
                result.close()

    def _read(self, result: Any, record: dict[str, Any],
              started: float) -> None:
        """Measure first-row latency separately from batch fetching."""
        record['phase'] = 'first_fetch'
        write_record(self.options.output, dict(record, event='phase'))
        logger.info("Probe '%s': Fetching first row.", record['table'])
        fetched = monotonic()
        try:
            first = result.fetchmany(1)
            record['first_fetch_seconds'] = monotonic() - fetched
            record['rows_fetched'] = len(first)
            if record['kind'] == 'decode':
                record['decoded_seeds'] = first[0][0] if first else 0
                record['status'] = 'complete'
            else:
                self._fetch(result, record, started)
        finally:
            # Failed calls need their timings too, before connection cleanup.
            record['fetch_seconds'] = monotonic() - fetched
            if 'first_fetch_seconds' not in record:
                record['first_fetch_seconds'] = record['fetch_seconds']

    def _fetch(self, result: Any, record: dict[str, Any],
               started: float) -> None:
        """Consume rows up to the row or elapsed-time budget."""
        record['phase'] = 'fetch'
        record['status'] = 'complete'
        write_record(self.options.output, dict(record, event='phase'))
        while record['rows_fetched']:
            # A zero row limit requests complete consumption.
            remaining = self.options.max_rows - record['rows_fetched']
            if self.options.max_rows and remaining <= 0:
                record['status'] = 'row_limit'
                break
            if monotonic() - started >= self.options.timeout:
                record['status'] = 'time_limit'
                break

            # Discard every batch immediately, without writing patient data.
            size = min(10000, remaining) if self.options.max_rows else 10000
            batch = result.fetchmany(size)
            if not batch:
                break
            record['rows_fetched'] += len(batch)
            logger.info("Probe '%s': Fetched %d rows.", record['table'],
                        record['rows_fetched'])
