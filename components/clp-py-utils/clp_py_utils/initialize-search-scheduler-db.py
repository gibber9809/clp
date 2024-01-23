#!/usr/bin/env python3
import argparse
import logging
import sys
from contextlib import closing

from clp_py_utils.clp_config import Database, SEARCH_JOBS_TABLE_NAME
from clp_py_utils.core import read_yaml_config_file
from job_orchestration.search_scheduler.common import JobStatus
from sql_adapter import SQL_Adapter

# Setup logging
# Create logger
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)
# Setup console logging
logging_console_handler = logging.StreamHandler()
logging_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
logging_console_handler.setFormatter(logging_formatter)
logger.addHandler(logging_console_handler)


def main(argv):
    args_parser = argparse.ArgumentParser(description="Sets up metadata tables for job orchestration.")
    args_parser.add_argument('--config', required=True, help="Database config file.")
    parsed_args = args_parser.parse_args(argv[1:])

    try:
        database_config = Database.parse_obj(read_yaml_config_file(parsed_args.config))
        if database_config is None:
            raise ValueError(f"Database configuration file '{parsed_args.config}' is empty.")
        sql_adapter = SQL_Adapter(database_config)
        with closing(sql_adapter.create_connection(True)) as scheduling_db, \
                closing(scheduling_db.cursor(dictionary=True)) as scheduling_db_cursor:
            scheduling_db_cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS `{SEARCH_JOBS_TABLE_NAME}` (
                    `id` INT NOT NULL AUTO_INCREMENT,
                    `status` INT NOT NULL DEFAULT '{JobStatus.PENDING}',
                    `submission_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                    `search_config` VARBINARY(60000) NOT NULL,
                    PRIMARY KEY (`id`) USING BTREE,
                    INDEX `JOB_STATUS` (`status`) USING BTREE
                ) ROW_FORMAT=DYNAMIC
            """)

            scheduling_db.commit()
    except:
        logger.exception("Failed to create search scheduler tables.")
        return -1

    return 0


if '__main__' == __name__:
    sys.exit(main(sys.argv))