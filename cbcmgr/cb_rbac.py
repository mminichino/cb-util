##
##

from __future__ import annotations
from typing import Optional, List
import attr


@attr.s
class CouchbaseRole:
    name: Optional[str] = attr.ib(default=None)
    bucket: Optional[str] = attr.ib(default="*")
    scope: Optional[str] = attr.ib(default=None)
    collection: Optional[str] = attr.ib(default=None)


@attr.s
class CouchbaseGroup:
    name: Optional[str] = attr.ib(default=None)
    description: Optional[str] = attr.ib(default=None)
    roles: Optional[List[CouchbaseRole]] = attr.ib(default=[])


@attr.s
class CouchbaseUser:
    username: Optional[str] = attr.ib(default=None)
    name: Optional[str] = attr.ib(default=None)
    password: Optional[str] = attr.ib(default=None)
    roles: Optional[List[CouchbaseRole]] = attr.ib(default=[])
    groups: Optional[List[str]] = attr.ib(default=[])
