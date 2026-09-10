from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path

from clp_py_utils.clp_config import ArchiveOutput, StorageType

# Constants
DATASET_NAME_MAX_LEN = 255

# An archive whose timestamp range is wider than this is also recorded in the long-span archives
# table, so that time-ranged queries can still find it after the archives table is partitioned.
# TODO: Make this configurable.
LONG_SPAN_THRESHOLD_MILLIS = 24 * 60 * 60 * 1000

# Archives whose timestamp range begins outside this range are quarantined in the archives table's
# `p_bad_low`/`p_bad_high` partitions, so that an implausible timestamp can't define a time-range
# partition boundary.
MIN_VALID_TIMESTAMP_MILLIS = 946_684_800_000  # 2000-01-01T00:00:00Z
MAX_VALID_TIMESTAMP_MILLIS = 4_102_444_800_000  # 2100-01-01T00:00:00Z

# The time range covered by each of the archives table's time-range partitions.
PARTITION_WINDOW_MILLIS = 24 * 60 * 60 * 1000

# MySQL's limit on the number of partitions in a table.
MAX_NUM_PARTITIONS = 8192

BAD_HIGH_PARTITION_NAME = "p_bad_high"
BAD_LOW_PARTITION_NAME = "p_bad_low"
FLOOR_PARTITION_NAME = "p_floor"
FUTURE_PARTITION_NAME = "p_future"

_FIXED_PARTITION_NAMES = frozenset(
    {
        BAD_HIGH_PARTITION_NAME,
        BAD_LOW_PARTITION_NAME,
        FLOOR_PARTITION_NAME,
        FUTURE_PARTITION_NAME,
    }
)

ARCHIVES_TABLE_SUFFIX = "archives"
COLUMN_METADATA_TABLE_SUFFIX = "column_metadata"
DATASETS_TABLE_SUFFIX = "datasets"
FILES_TABLE_SUFFIX = "files"
LONG_SPAN_ARCHIVES_TABLE_SUFFIX = "long_span_archives"


def _create_archives_table(db_cursor, archives_table_name: str) -> None:
    # NOTE: `archive_uuid` can't be a unique key, since MySQL requires every unique key of a
    # partitioned table to contain the partitioning column.
    db_cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS `{archives_table_name}` (
            `id` BIGINT unsigned NOT NULL AUTO_INCREMENT,
            `dataset_id` SMALLINT unsigned NOT NULL,
            `uuid` BINARY(16) NOT NULL,
            `timestamp_range_begin_millis` BIGINT NOT NULL,
            `timestamp_range_end_millis` BIGINT NOT NULL,
            `num_uncompressed_bytes` BIGINT unsigned NOT NULL,
            `num_compressed_bytes` BIGINT unsigned NOT NULL,
            `creation_time_millis` BIGINT NOT NULL,
            `pack_id` BIGINT unsigned DEFAULT NULL,
            `is_deleted` BOOLEAN NOT NULL DEFAULT FALSE,
            PRIMARY KEY (`dataset_id`, `timestamp_range_begin_millis`, `id`),
            KEY `auto_inc` (`id`),
            KEY `archive_uuid` (`dataset_id`, `uuid`),
            KEY `archives_gc_order` (`dataset_id`, `creation_time_millis`)
        ) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8
        PARTITION BY RANGE (`timestamp_range_begin_millis`) (
            PARTITION `p_bad_low` VALUES LESS THAN ({MIN_VALID_TIMESTAMP_MILLIS}),
            PARTITION `p_future` VALUES LESS THAN ({MAX_VALID_TIMESTAMP_MILLIS}),
            PARTITION `p_bad_high` VALUES LESS THAN MAXVALUE
        )
        """
    )


def _create_long_span_archives_table(db_cursor, table_prefix: str) -> None:
    # NOTE: This table holds a second reference to every archive whose timestamp range exceeds the
    # span contract, so it's deliberately unpartitioned.
    db_cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS `{get_long_span_archives_table_name(table_prefix)}` (
            `dataset_id` SMALLINT unsigned NOT NULL,
            `timestamp_range_begin_millis` BIGINT NOT NULL,
            `timestamp_range_end_millis` BIGINT NOT NULL,
            `archive_id` BIGINT unsigned NOT NULL,
            PRIMARY KEY (`dataset_id`, `timestamp_range_end_millis`, `archive_id`),
            KEY `by_begin` (`dataset_id`, `timestamp_range_begin_millis`)
        ) ENGINE=InnoDB
        """
    )


def _create_column_metadata_table(db_cursor, table_prefix: str) -> None:
    db_cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS `{get_column_metadata_table_name(table_prefix)}` (
            `dataset_id` SMALLINT unsigned NOT NULL,
            `name` VARCHAR(512) NOT NULL,
            `type` TINYINT NOT NULL,
            PRIMARY KEY (`dataset_id`, `name`, `type`)
        )
        """
    )


def _get_table_name(prefix: str, suffix: str) -> str:
    """
    :param prefix:
    :param suffix:
    :return: The table name in the form of "<prefix><suffix>".
    """
    return prefix + suffix


def create_datasets_table(db_cursor, table_prefix: str) -> None:
    """
    Creates the datasets information table.

    :param db_cursor: The database cursor to execute the table creation.
    :param table_prefix: A string to prepend to the table name.
    """
    # For a description of the table, see
    # `../../../docs/src/dev-docs/design-metadata-db.md`
    db_cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS `{get_datasets_table_name(table_prefix)}` (
            `id` SMALLINT unsigned NOT NULL AUTO_INCREMENT,
            `name` VARCHAR(255) NOT NULL,
            `archive_storage_path` VARCHAR(4096) NOT NULL,
            `retention_period_minutes` INT unsigned DEFAULT NULL,
            `is_deleted` BOOLEAN NOT NULL DEFAULT FALSE,
            UNIQUE KEY `dataset_name` (`name`) USING BTREE,
            PRIMARY KEY (`id`)
        )
        """
    )


def add_dataset(
    db_conn,
    db_cursor,
    table_prefix: str,
    dataset_name: str,
    archive_output: ArchiveOutput,
) -> int:
    """
    Inserts a new dataset into the `datasets` table.

    :param db_conn:
    :param db_cursor: The database cursor to execute the table row insertion.
    :param table_prefix: A string to prepend to the table name.
    :param dataset_name:
    :param archive_output:
    :return: The ID of the newly inserted dataset.
    """
    archive_storage_directory: Path
    if StorageType.S3 == archive_output.storage.type:
        s3_config = archive_output.storage.s3_config
        archive_storage_directory = Path(s3_config.key_prefix)
    else:
        archive_storage_directory = archive_output.get_directory()

    query = f"""INSERT INTO `{get_datasets_table_name(table_prefix)}`
                (name, archive_storage_path)
                VALUES (%s, %s)
                """
    db_cursor.execute(
        query,
        (dataset_name, str(archive_storage_directory / dataset_name)),
    )
    # NOTE: `lastrowid` is only valid until the next statement runs on this cursor.
    dataset_id = int(db_cursor.lastrowid)
    db_conn.commit()

    return dataset_id


def fetch_existing_datasets(
    db_cursor,
    table_prefix: str,
) -> dict[str, int]:
    """
    Gets the names and IDs of all existing datasets.

    :param db_cursor:
    :param table_prefix:
    :return: A map of each dataset's name to its ID.
    """
    db_cursor.execute(
        f"""
        SELECT id, name FROM `{get_datasets_table_name(table_prefix)}`
        WHERE is_deleted = FALSE
        """
    )
    rows = db_cursor.fetchall()
    return {row["name"]: row["id"] for row in rows}


def create_metadata_db_tables(db_cursor, table_prefix: str) -> None:
    """
    Creates the standard set of tables for CLP's metadata.

    The tables are shared by every dataset, so they only need to be created once.

    :param db_cursor: The database cursor to execute the table creations.
    :param table_prefix: A string to prepend to all table names.
    """
    archives_table_name = get_archives_table_name(table_prefix)

    _create_archives_table(db_cursor, archives_table_name)
    _create_long_span_archives_table(db_cursor, table_prefix)
    _create_column_metadata_table(db_cursor, table_prefix)


def delete_archives_from_metadata_db(
    db_cursor, archive_ids: list[int], table_prefix: str, dataset_id: int
) -> None:
    """
    Deletes archives from the metadata database specified by a list of IDs. It also deletes the
    associated entries from the `long_span_archives` table that reference these archives.

    The order of deletion follows the foreign key constraints, ensuring no violations occur during
    the process.

    :param db_cursor:
    :param archive_ids: The list of archive to delete.
    :param table_prefix:
    :param dataset_id:
    """
    if 0 == len(archive_ids):
        return

    ids_list_string = ", ".join(["%s"] * len(archive_ids))
    params = [dataset_id, *archive_ids]

    db_cursor.execute(
        f"""
        DELETE FROM `{get_long_span_archives_table_name(table_prefix)}`
        WHERE dataset_id = %s AND archive_id in ({ids_list_string})
        """,
        params,
    )

    db_cursor.execute(
        f"""
        DELETE FROM `{get_archives_table_name(table_prefix)}`
        WHERE dataset_id = %s AND id in ({ids_list_string})
        """,
        params,
    )


def delete_dataset_from_metadata_db(db_cursor, table_prefix: str, dataset: str) -> None:
    """
    Marks `dataset` as deleted in the metadata database.

    The dataset's rows in the other metadata tables are left for the garbage collector to reclaim,
    and the dataset continues to occupy its name.

    :param db_cursor:
    :param table_prefix:
    :param dataset:
    """
    db_cursor.execute(
        f"""
        UPDATE `{get_datasets_table_name(table_prefix)}`
        SET is_deleted = TRUE
        WHERE name = %s
        """,
        (dataset,),
    )


def get_archives_table_name(table_prefix: str) -> str:
    return _get_table_name(table_prefix, ARCHIVES_TABLE_SUFFIX)


def get_column_metadata_table_name(table_prefix: str) -> str:
    return _get_table_name(table_prefix, COLUMN_METADATA_TABLE_SUFFIX)


def get_datasets_table_name(table_prefix: str) -> str:
    return _get_table_name(table_prefix, DATASETS_TABLE_SUFFIX)


def get_long_span_archives_table_name(table_prefix: str) -> str:
    return _get_table_name(table_prefix, LONG_SPAN_ARCHIVES_TABLE_SUFFIX)


def get_files_table_name(table_prefix: str) -> str:
    # TODO: The files tables aren't created yet since their schema is still being settled.
    return _get_table_name(table_prefix, FILES_TABLE_SUFFIX)


def _get_partitions(db_cursor, table_name: str) -> list[tuple[str, int | None]]:
    """
    :param db_cursor:
    :param table_name:
    :return: A list of (name, upper bound) tuples ordered by the partitions' positions in
    `table_name`, where the upper bound is `None` for the partition holding `MAXVALUE`.
    """
    db_cursor.execute(
        """
        SELECT PARTITION_NAME AS name, PARTITION_DESCRIPTION AS description
        FROM information_schema.PARTITIONS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
        ORDER BY PARTITION_ORDINAL_POSITION
        """,
        (table_name,),
    )

    partitions: list[tuple[str, int | None]] = []
    for row in db_cursor.fetchall():
        description = row["description"]
        upper_bound = None if "MAXVALUE" == description else int(description)
        partitions.append((row["name"], upper_bound))

    return partitions


def _get_partition_name(upper_bound_millis: int) -> str:
    """
    :param upper_bound_millis:
    :return: The name for the partition whose upper bound is `upper_bound_millis`, derived from the
    day the partition covers.
    """
    window_begin_secs = (upper_bound_millis - PARTITION_WINDOW_MILLIS) / 1000
    return "p_" + datetime.fromtimestamp(window_begin_secs, tz=timezone.utc).strftime("%Y%m%d")


def add_archive_partitions(db_cursor, table_prefix: str, num_lookahead_days: int) -> list[str]:
    """
    Extends the archives table's time-range partitions so that they cover every day from the last
    partitioned day up to `num_lookahead_days` after the current day.

    The partitions are carved out of the table's future partition. If no time-range partitions
    exist yet, the floor partition is created (or extended) up to the current day so that the first
    time-range partition doesn't span every timestamp below it.

    :param db_cursor:
    :param table_prefix:
    :param num_lookahead_days:
    :return: The names of the partitions that were added.
    :raise ValueError: If adding the partitions would exceed `MAX_NUM_PARTITIONS`.
    """
    archives_table_name = get_archives_table_name(table_prefix)
    partitions = _get_partitions(db_cursor, archives_table_name)
    partition_names = {name for name, _ in partitions}
    window_upper_bounds = [
        upper_bound for name, upper_bound in partitions if name not in _FIXED_PARTITION_NAMES
    ]

    current_millis = int(time.time() * 1000)
    current_day_begin = current_millis - current_millis % PARTITION_WINDOW_MILLIS
    target_upper_bound = current_day_begin + (num_lookahead_days + 1) * PARTITION_WINDOW_MILLIS

    floor_upper_bound: int | None = None
    if 0 == len(window_upper_bounds):
        floor_upper_bound = current_day_begin
        next_upper_bound = current_day_begin + PARTITION_WINDOW_MILLIS
    else:
        next_upper_bound = max(window_upper_bounds) + PARTITION_WINDOW_MILLIS

    new_upper_bounds: list[int] = []
    while next_upper_bound <= target_upper_bound:
        new_upper_bounds.append(next_upper_bound)
        next_upper_bound += PARTITION_WINDOW_MILLIS
    if 0 == len(new_upper_bounds):
        return []

    num_partitions = len(partitions) + len(new_upper_bounds)
    if FLOOR_PARTITION_NAME not in partition_names:
        num_partitions += 1
    if MAX_NUM_PARTITIONS < num_partitions:
        raise ValueError(
            f"Adding {len(new_upper_bounds)} partition(s) to `{archives_table_name}` would exceed"
            f" the maximum of {MAX_NUM_PARTITIONS}."
        )

    partitions_to_reorganize = [FUTURE_PARTITION_NAME]
    new_partition_definitions: list[str] = []
    if floor_upper_bound is not None:
        if FLOOR_PARTITION_NAME in partition_names:
            partitions_to_reorganize.insert(0, FLOOR_PARTITION_NAME)
        new_partition_definitions.append(
            f"PARTITION `{FLOOR_PARTITION_NAME}` VALUES LESS THAN ({floor_upper_bound})"
        )

    new_partition_names = [_get_partition_name(bound) for bound in new_upper_bounds]
    for name, upper_bound in zip(new_partition_names, new_upper_bounds):
        new_partition_definitions.append(
            f"PARTITION `{name}` VALUES LESS THAN ({upper_bound})"
        )
    new_partition_definitions.append(
        f"PARTITION `{FUTURE_PARTITION_NAME}` VALUES LESS THAN ({MAX_VALID_TIMESTAMP_MILLIS})"
    )

    reorganized = ", ".join(f"`{name}`" for name in partitions_to_reorganize)
    db_cursor.execute(
        f"ALTER TABLE `{archives_table_name}` REORGANIZE PARTITION {reorganized} INTO"
        f" ({', '.join(new_partition_definitions)})"
    )

    return new_partition_names


def drop_empty_archive_partitions(db_cursor, table_prefix: str) -> list[str]:
    """
    Drops the oldest run of the archives table's time-range partitions that contain no archives.

    Only the oldest contiguous run is dropped, since dropping a partition merges its time range
    into the partition above it, which would widen a partition that still holds archives.

    :param db_cursor:
    :param table_prefix:
    :return: The names of the partitions that were dropped.
    """
    archives_table_name = get_archives_table_name(table_prefix)

    empty_partition_names: list[str] = []
    for name, _ in _get_partitions(db_cursor, archives_table_name):
        if name in _FIXED_PARTITION_NAMES:
            continue

        db_cursor.execute(f"SELECT 1 FROM `{archives_table_name}` PARTITION (`{name}`) LIMIT 1")
        if db_cursor.fetchone() is not None:
            break

        empty_partition_names.append(name)

    if 0 == len(empty_partition_names):
        return []

    dropped = ", ".join(f"`{name}`" for name in empty_partition_names)
    db_cursor.execute(f"ALTER TABLE `{archives_table_name}` DROP PARTITION {dropped}")

    return empty_partition_names
