from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import Table

from google.cloud.datacatalog_v1.types import PolicyTag
from google.cloud.datacatalog_v1 import (
    PolicyTagManagerClient,
    GetPolicyTagRequest
)

from google.api_core.exceptions import NotFound

from typing import List, Dict

import constants


class Source():

    def __init__(
        self,
        project: str,
        dataset: str,
        table_name: str
    ):
        self.project: str = project
        self.dataset: str = dataset
        self.table_name: str = table_name
        self.table: Table
        self.policy_tag_client: PolicyTagManagerClient

    def __str__(self):
        return (
            f"\n- Project: {self.project}"
            f"\n- Dataset: {self.dataset}"
            f"\n- Table: {self.table_name}"
        )

    def get_table(self) -> None:
        bq_client = bigquery.Client(project=self.project)
        table_id = f'{self.project}.{self.dataset}.{self.table_name}'

        try:
            table = bq_client.get_table(table_id)
            self.table = table
        except NotFound as e:
            raise Exception(
                "Could not retrive BigQuery table information: {}", e
            )

    def get_table_schema(self) -> List[SchemaField]:
        self.get_table()
        return self.table.schema

    def get_column_policy_tag_mapping(self) -> Dict[str, List[str]]:
        mapping = {}
        table_schema = self.get_table_schema()

        for field in table_schema:
            api_repr = field.to_api_repr()
            key = field.name
            parent_value = api_repr.get(constants.API_REPR_POLICY_TAG_KEY, None)
            value = []
            if parent_value:
                value = parent_value.get(constants.API_REPR_POLICY_TAG_VALUE, [])
            mapping.update({key: value})

        print(mapping)

        return mapping

    def create_tag_api_client(self) -> None:
        client = PolicyTagManagerClient()
        self.policy_tag_client = client

    def get_policy_tag(self, policy_tag_name: str) -> PolicyTag:
        request = GetPolicyTagRequest(
            name=policy_tag_name,
        )

        policy_tag = self.policy_tag_client.get_policy_tag(request=request)

        return policy_tag
