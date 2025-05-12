CREATE TABLE `{project-id}.{dataset-id}.policy_tag_mapping_table`(
    project_id STRING,
    dataset_id STRING,
    table_name STRING,
    column_name STRING,
    policy_tag_uri STRING
)
CLUSTER BY project_id, dataset_id, table_name, column_name
OPTIONS(
    description="A table for storing the mapping between column names and polii tags URIs ",
    labels=[("policy_tags","other_label")]
)

-- Record type needs to be specified at leaf level
-- Table update doesn't need to delete the table.
-- Data type requires different 