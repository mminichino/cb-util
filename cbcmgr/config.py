##
##

from __future__ import annotations
import attr
from enum import Enum
from typing import List


class KeyStyle(Enum):
    DEFAULT = 0
    TYPE = 1
    UUID = 2
    FIELD = 3
    COLLECTION = 4
    COMPOUND = 5
    PATH = 6
    TEXT = 7
    TEXT_FIELD = 8


class MapUpsertType(Enum):
    DOCUMENT = 0
    LIST = 1


@attr.s
class UpsertMapConfig:
    paths: List[UpsertMapPathConfig] = attr.ib(default=[])

    @classmethod
    def new(cls):
        return cls(
            []
        )

    def add(self,
            path: str,
            p_type: MapUpsertType = MapUpsertType.DOCUMENT,
            collection: bool = False,
            exclude: list[str] = None,
            doc_id: KeyStyle = KeyStyle.TEXT,
            id_key: str = "record_id",
            optional: bool = False):
        self.paths.append(
            UpsertMapPathConfig.create(
                path,
                p_type,
                collection,
                exclude,
                doc_id,
                id_key,
                optional
            )
        )

    def get(self) -> list[UpsertMapPathConfig]:
        return self.__dict__['paths']


@attr.s
class UpsertMapPathConfig:
    path: str = attr.ib()
    p_type: MapUpsertType = attr.ib(default=MapUpsertType.DOCUMENT)
    collection: bool = attr.ib(default=False)
    exclude: list[str] = attr.ib(default=None)
    id: KeyStyle = attr.ib(default=KeyStyle.TEXT)
    id_key: str = attr.ib(default="record_id")
    optional: bool = attr.ib(default=False)

    @classmethod
    def create(cls,
               path: str,
               p_type: MapUpsertType,
               collection: bool,
               exclude: list[str],
               doc_id: KeyStyle,
               id_key: str,
               optional: bool):
        return cls(
            path,
            p_type,
            collection,
            exclude,
            doc_id,
            id_key,
            optional
        )

    @property
    def name(self):
        return self.path.split('.')[-1]
