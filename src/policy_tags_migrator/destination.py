import constants
import util

from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.schema import PolicyTagList
from google.cloud.bigquery.table import RowIterator

from google.api_core.exceptions import NotFound

from typing import List, Dict, Any


class Destination():

    def __init__(
        self,
        project: str,
        dataset: str,
        table_name: str
    ):
        self.project: str = project
        self.dataset: str = dataset
        self.table_name: str = table_name
        self.bq_client = bigquery.Client(project=project)

    def __str__(self):
        return (
            f"\n- Project: {self.project}"
            f"\n- Dataset: {self.dataset}"
            f"\n- Table: {self.table_name}"
        )

    def set_table(self) -> None:
        table_id = f'{self.project}.{self.dataset}.{self.table_name}'

        try:
            table = self.bq_client.get_table(table=table_id)
            self.table = table
        except NotFound as e:
            raise Exception(
                "Could not retrive BigQuery table information: {}",
                e
            )

    def get_table_schema(self) -> List[SchemaField]:
        self.set_table()
        return self.table.schema

    def tableschema_to_dict(self) -> List[Dict[str, Any]]:
        table_schema = self.get_table_schema()
        fields_list = []

        for field in table_schema:
            fields_list.append(field.__dict__.get('_properties'))

        if len(fields_list) > 0:
            return fields_list
        else:
            raise Exception(
                f"The table {self.table.friendly_name} has no fields"
            )

    def create_table_schema(
            self,
            column_mapping: Dict[str, List[str]]
    ) -> List[SchemaField]:

        table_schema = self.get_table_schema()
        new_table_schema = []

        for field in table_schema:
            api_repr = field.to_api_repr()
            api_repr_with_policy_tags = util.add_policy_tag_to_schema(
                api_repr_dict=api_repr,
                policy_tags=column_mapping.get(field.name)
            )

            new_field = field.from_api_repr(api_repr=api_repr_with_policy_tags)

            new_table_schema.append(new_field)

        return new_table_schema

    def get_column_policy_tag_mapping(
            self,
            mapping_data: RowIterator
    ) -> Dict[str, List[str]]:
        mapping_raw = {}

        for row in mapping_data:
            key = row.get(constants.BQ_MAPPING_TABLE_COL_COLUMN_NAME)
            value = mapping_raw.get(key, [])
            value.append(
                row.get(constants.BQ_MAPPING_TABLE_POLICY_TAG_COLUMN_NAME)
            )
            mapping_raw.update({key: value})

        return mapping_raw

    def update_table_schema(self, column_mapping: Dict[str, List[str]]):
        new_table_schema = self.create_table_schema(
            column_mapping=column_mapping
        )

        self.table.schema = new_table_schema

        self.table = self.bq_client.update_table(
            table=self.table,
            fields=[constants.SCHEMA_TABLE_PROPERTIES]
        )

    def apply_policy_tags_from_mapping_table(self, mapping_data: RowIterator):
        policy_tag_mapping = self.get_column_policy_tag_mapping(
            mapping_data=mapping_data
        )

        self.update_table_schema(column_mapping=policy_tag_mapping)
