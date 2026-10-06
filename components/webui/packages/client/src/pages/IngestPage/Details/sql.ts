import {Nullable} from "@webui/common/utility-types";

import {querySql} from "../../../api/sql";
import {settings} from "../../../settings";
import {
    buildDatasetNameList,
    CLP_ARCHIVES_TABLE_COLUMN_NAMES,
    CLP_DATASETS_TABLE_COLUMN_NAMES,
    CLP_FILES_TABLE_COLUMN_NAMES,
    CLP_S_ARCHIVES_TABLE_COLUMN_NAMES,
} from "../sqlConfig";


/**
 * Result from SQL details query.
 */
interface DetailsItem {
    begin_timestamp: Nullable<number>;
    end_timestamp: Nullable<number>;
    num_files: Nullable<number>;
    num_messages: Nullable<number>;
}

/**
 * Row returned by the clp-s details query, which only covers the archives' time range.
 */
interface DetailsTimeRangeRow {
    begin_timestamp: Nullable<number>;
    end_timestamp: Nullable<number>;
}

/**
 * Default values for details when no data is available.
 */
const DETAILS_DEFAULT: DetailsItem = {
    begin_timestamp: null,
    end_timestamp: null,
    num_files: 0,
    num_messages: 0,
};

/**
 * Builds the query string for details stats when using CLP storage engine (i.e. no datasets).
 *
 * @return
 */
const getDetailsSql = () => `
SELECT
    a.begin_timestamp         AS begin_timestamp,
    a.end_timestamp           AS end_timestamp,
    b.num_files               AS num_files,
    b.num_messages            AS num_messages
FROM
(
    SELECT
        MIN(${CLP_ARCHIVES_TABLE_COLUMN_NAMES.BEGIN_TIMESTAMP})   AS begin_timestamp,
        MAX(${CLP_ARCHIVES_TABLE_COLUMN_NAMES.END_TIMESTAMP})     AS end_timestamp
    FROM ${settings.SqlDbClpArchivesTableName}
) a,
(
    SELECT
        COUNT(DISTINCT ${CLP_FILES_TABLE_COLUMN_NAMES.ORIG_FILE_ID})   AS num_files,
        CAST(
            COALESCE(
                SUM(${CLP_FILES_TABLE_COLUMN_NAMES.NUM_MESSAGES}),
                0
            ) AS UNSIGNED
        ) AS num_messages
    FROM ${settings.SqlDbClpFilesTableName}
) b;
`;

/**
 * Builds the query string for details stats when using CLP-S storage engine
 * (i.e. multiple datasets).
 *
 * NOTE: `num_files` and `num_messages` aren't queried, since the clp-s details panel only renders
 * the time range and the files tables don't exist in the shared schema yet.
 *
 * @param datasetNames
 * @return
 */
const buildMultiDatasetDetailsSql = (datasetNames: string[]): string => `
    SELECT
      MIN(archives.${CLP_S_ARCHIVES_TABLE_COLUMN_NAMES.TIMESTAMP_RANGE_BEGIN_MILLIS})
        AS begin_timestamp,
      MAX(archives.${CLP_S_ARCHIVES_TABLE_COLUMN_NAMES.TIMESTAMP_RANGE_END_MILLIS})
        AS end_timestamp
    FROM ${settings.SqlDbClpArchivesTableName} AS archives
    JOIN ${settings.SqlDbClpDatasetsTableName} AS datasets
      ON archives.${CLP_S_ARCHIVES_TABLE_COLUMN_NAMES.DATASET_ID} =
         datasets.${CLP_DATASETS_TABLE_COLUMN_NAMES.ID}
    WHERE datasets.${CLP_DATASETS_TABLE_COLUMN_NAMES.NAME}
      IN (${buildDatasetNameList(datasetNames)});
  `;

/**
 * Executes a details SQL query and extracts its single row.
 *
 * @param sql
 * @return
 * @throws {Error} if query result does not contain data
 */
const executeSingleRowQuery = async <T>(sql: string): Promise<T> => {
    const resp = await querySql<T[]>(sql);
    const [row] = resp.data;
    if ("undefined" === typeof row) {
        throw new Error("Details result does not contain data.");
    }

    return row;
};

/**
 * Fetches details statistics when using CLP storage engine.
 *
 * @return
 */
const fetchClpDetails = async (): Promise<DetailsItem> => {
    const sql = getDetailsSql();
    return executeSingleRowQuery<DetailsItem>(sql);
};

/**
 * Fetches details statistics when using CLP-S storage engine.
 *
 * @param datasetNames
 * @return
 */
const fetchClpsDetails = async (
    datasetNames: string[]
): Promise<DetailsItem> => {
    if (0 === datasetNames.length) {
        return DETAILS_DEFAULT;
    }
    const sql = buildMultiDatasetDetailsSql(datasetNames);
    const timeRange = await executeSingleRowQuery<DetailsTimeRangeRow>(sql);

    return {
        ...DETAILS_DEFAULT,
        begin_timestamp: timeRange.begin_timestamp,
        end_timestamp: timeRange.end_timestamp,
    };
};

export type {DetailsItem};
export {
    DETAILS_DEFAULT,
    fetchClpDetails,
    fetchClpsDetails,
};
