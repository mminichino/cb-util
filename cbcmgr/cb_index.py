##
##

from __future__ import annotations
from typing import Protocol, List, Optional
import attr


@attr.s
class CBQueryIndex(Protocol):
    name: Optional[str] = attr.ib(default=None)
    is_primary: Optional[bool] = attr.ib(default=False)
    state: Optional[str] = attr.ib(default=None)
    namespace_id: Optional[str] = attr.ib(default=None)
    keyspace_id: Optional[str] = attr.ib(default=None)
    index_key: Optional[List[str]] = attr.ib(default=None)
    condition: Optional[str] = attr.ib(default=None)
    bucket_id: Optional[str] = attr.ib(default=None)
    scope_id: Optional[str] = attr.ib(default=None)
    using: Optional[str] = attr.ib(default=None)
    datastore_id: Optional[str] = attr.ib(default=None)
    num_replica: Optional[int] = attr.ib(default=0)

    @classmethod
    def from_dict(cls, json_data):
        return cls(json_data.get("name"),
                   bool(json_data.get("is_primary")),
                   json_data.get("state"),
                   json_data.get("namespace_id"),
                   json_data.get("keyspace_id"),
                   json_data.get("index_key", []),
                   json_data.get("condition"),
                   json_data.get("bucket_id"),
                   json_data.get("scope_id"),
                   json_data.get("using"),
                   json_data.get("datastore_id"),
                   json_data.get("metadata", {}).get('num_replica', 0)
                   )
