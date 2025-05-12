import logging

from google.cloud import bigquery
from google.cloud.bigquery.table import (
    RowIterator, _EmptyRowIterator
)

logging.basicConfig(
    format="%(asctime)s - %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

bq_client = bigquery.Client()

job_config = bigquery.QueryJobConfig(
    # Run at batch priority, which won't count toward concurrent rate limit.
    priority=bigquery.QueryPriority.BATCH
)


def get_mapping_data(
        mapping_tablename: str,
        project_id: str,
        dataset_id: str,
        table_name: str
) -> (RowIterator | _EmptyRowIterator):
    QUERY = (
        f"SELECT * FROM {mapping_tablename} "
        f"WHERE project_id='{project_id}' "
        f"AND dataset_id='{dataset_id}' "
        f"AND table_name='{table_name}';"
    )
    query_job = bq_client.query(query=QUERY, job_config=job_config)
    logger.info(
        "Retieving mapping information with Job ID: %s", query_job.job_id
    )
    rows = query_job.result()

    return rows
