##
##

from __future__ import annotations
from datetime import timedelta
from typing import Optional
import attr
from couchbase.durability import DurabilityLevel
from couchbase.management.logic.buckets_logic import BucketType, CompressionMode, ConflictResolutionType, EvictionPolicyType, StorageBackend


@attr.s
class Bucket:
    flush_enabled: Optional[bool] = attr.ib(default=False)
    num_replicas: Optional[int] = attr.ib(default=1)
    ram_quota_mb: Optional[int] = attr.ib(default=128)
    replica_index: Optional[bool] = attr.ib(default=False)
    bucket_type: Optional[BucketType] = attr.ib(default=BucketType.COUCHBASE)
    max_ttl: Optional[int] = attr.ib(default=0)
    max_expiry: Optional[timedelta] = attr.ib(default=timedelta(0))
    compression_mode: Optional[CompressionMode] = attr.ib(default=CompressionMode.PASSIVE)
    conflict_resolution_type: Optional[ConflictResolutionType] = attr.ib(default=ConflictResolutionType.SEQUENCE_NUMBER)
    eviction_policy: Optional[EvictionPolicyType] = attr.ib(default=EvictionPolicyType.VALUE_ONLY)
    name: Optional[str] = attr.ib(default=None)
    minimum_durability_level: Optional[DurabilityLevel] = attr.ib(default=DurabilityLevel.NONE)
    storage_backend: Optional[StorageBackend] = attr.ib(default=StorageBackend.COUCHSTORE)
