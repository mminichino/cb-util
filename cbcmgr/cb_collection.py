##
##

from __future__ import annotations
from datetime import timedelta
from typing import Optional
import attr


@attr.s
class Collection:
    name: Optional[str] = attr.ib(default=None)
    max_ttl: Optional[timedelta] = attr.ib(default=timedelta(0))
