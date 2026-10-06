import asyncio
from contextlib import closing

from clp_py_utils.clp_config import ClpConfig, Database
from clp_py_utils.clp_logging import configure_logging, get_logger
from clp_py_utils.clp_metadata_db_utils import (
    add_archive_partitions,
    drop_empty_archive_partitions,
)
from clp_py_utils.sql_adapter import SqlAdapter

from job_orchestration.garbage_collector.constants import (
    ARCHIVE_PARTITION_MAINTAINER_NAME,
    MIN_TO_SECONDS,
)

logger = get_logger(ARCHIVE_PARTITION_MAINTAINER_NAME)


def _maintain_archive_partitions(
    database_config: Database,
    num_lookahead_days: int,
) -> None:
    """
    Reclaims the archives table's expired time-range partitions, then extends its partitions to
    cover the days ahead.

    The partitions are reclaimed before they're extended so that a table which has reached the
    partition limit can recover: dropping the partitions that are no longer needed is what frees the
    capacity to add new ones.

    :param database_config:
    :param num_lookahead_days:
    """
    clp_connection_params = database_config.get_clp_connection_params_and_type()
    table_prefix = clp_connection_params["table_prefix"]
    sql_adapter = SqlAdapter(database_config)
    with (
        closing(sql_adapter.create_connection(True)) as db_conn,
        closing(db_conn.cursor(dictionary=True)) as db_cursor,
    ):
        dropped_partitions = drop_empty_archive_partitions(db_cursor, table_prefix)
        if 0 != len(dropped_partitions):
            logger.info(f"Dropped {len(dropped_partitions)} partition(s): {dropped_partitions}")

        added_partitions = add_archive_partitions(db_cursor, table_prefix, num_lookahead_days)
        if 0 != len(added_partitions):
            logger.info(f"Added {len(added_partitions)} partition(s): {added_partitions}")

        db_conn.commit()


async def archive_partition_maintainer(clp_config: ClpConfig) -> None:
    configure_logging(logger, ARCHIVE_PARTITION_MAINTAINER_NAME)

    garbage_collector_config = clp_config.garbage_collector
    maintenance_interval_secs = (
        garbage_collector_config.sweep_interval.archive_partition * MIN_TO_SECONDS
    )
    num_lookahead_days = garbage_collector_config.archive_partition_lookahead_days

    logger.info(f"{ARCHIVE_PARTITION_MAINTAINER_NAME} started.")
    try:
        while True:
            _maintain_archive_partitions(clp_config.database, num_lookahead_days)
            await asyncio.sleep(maintenance_interval_secs)
    except Exception:
        logger.exception(f"{ARCHIVE_PARTITION_MAINTAINER_NAME} exited with failure.")
        raise
