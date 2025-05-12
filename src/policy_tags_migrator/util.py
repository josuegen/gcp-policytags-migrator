import constants

from google.cloud.bigquery.schema import PolicyTagList

from google.cloud.datacatalog_v1 import (
    PolicyTagManagerClient,
    GetPolicyTagRequest
)
from google.cloud.datacatalog_v1.types import PolicyTag

from typing import List, Dict, Any


def policy_tag_from_uri(policy_tag_uri: str) -> PolicyTag:
    client = PolicyTagManagerClient()

    request = GetPolicyTagRequest(
        name=policy_tag_uri,
    )

    response = client.get_policy_tag(request=request)

    return response


def get_policy_tag_list(policy_tags: List[str]) -> PolicyTagList:
    policy_tag_tuple = tuple(policy_tags)
    policy_tag_list = PolicyTagList(names=policy_tag_tuple)

    return policy_tag_list


def add_policy_tag_to_schema(
        api_repr_dict: Dict[str, Any],
        policy_tags: List[str]
):
    new_api_repr = api_repr_dict

    new_api_repr[constants.API_REPR_POLICY_TAG_KEY] = {
        constants.API_REPR_POLICY_TAG_VALUE: policy_tags
    }

    return new_api_repr
