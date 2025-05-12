import argparse
import logging

from source import Source
from destination import Destination
import mapping_table
import validators


def copy_policy_tags(source: Source, destination: Destination):
    policy_tag_mapping = source.get_column_policy_tag_mapping()
    destination.update_table_schema(column_mapping=policy_tag_mapping)


def migrate_policy_tags():
    pass


if __name__ == '__main__':
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    bigquery_fqn = validators.BigQueryValidator()

    arguments = argparse.ArgumentParser()
    arguments.add_argument(
        '--source_table',
        '-st',
        required=True,
        type=bigquery_fqn
    )
    arguments.add_argument(
        '--target_table',
        '-tt',
        required=True,
        type=bigquery_fqn
    )
    arguments.add_argument('--migrate_tags', '-mt', action='store_true')
    arguments.add_argument('--simple_copy', '-sc', action='store_true')
    arguments.add_argument('--mapping_table', '-map', action='store_true')
    arguments.add_argument(
        '--bq_mapping_table',
        '-bq_mt',
        type=bigquery_fqn
    )

    args = arguments.parse_args()

    source_parts = args.source_table.split('.')
    dest_parts = args.target_table.split('.')

    source = Source(
        project=source_parts[0],
        dataset=source_parts[1],
        table_name=source_parts[2]
    )

    destination = Destination(
        project=dest_parts[0],
        dataset=dest_parts[1],
        table_name=dest_parts[2]
    )

    logger.info("Applying policy tags to %s", str(destination))

    if args.simple_copy:
        logger.info(f"From source table: {str(source)}")

        if args.migrate_tags:
            migrate_policy_tags()

        copy_policy_tags(source=source, destination=destination)

    elif args.mapping_table:
        logger.info(f"From mapping table: {args.bq_mapping_table}")

        mapping_data = mapping_table.get_mapping_data(
            mapping_tablename=args.bq_mapping_table,
            project_id=dest_parts[0],
            dataset_id=dest_parts[1],
            table_name=dest_parts[2]
        )
        logger.info(
            f"Found {mapping_data.total_rows} "
            "Policy Tag(s) to be applied to the target table."
        )

        destination.apply_policy_tags_from_mapping_table(
            mapping_data=mapping_data
        )

    else:
        logger.error(
            "Either source table or mapping table method should be used."
        )

    logger.info("Applied Policy Tag(s) successfully.")
