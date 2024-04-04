##
##

from __future__ import annotations
import attr
from typing import List, Optional, Union


@attr.s
class CBSearchIndex:
    doc_config: Optional[DocConfig] = attr.ib(default=None)
    mapping: Optional[MappingScoped] = attr.ib(default=None)

    @classmethod
    def create(cls,
               name: str = None,
               dims: Union[List[int], None] = None,
               vector_fields: Union[List[str], None] = None,
               similarity: str = "dot_product",
               text_field: Union[str, None] = None,
               default: bool = False,
               metadata: bool = False):
        if not vector_fields and not text_field:
            raise ValueError("Either the vector or text field parameter is required")
        if default:
            mapping = MappingDefault()
            if vector_fields:
                mapping.default_mapping.add_vector(vector_fields, dims, similarity)
            if text_field:
                mapping.default_mapping.add_text(text_field)
            if metadata:
                mapping.default_mapping.add_metadata()
        else:
            if not name:
                raise ValueError("Name parameter is required for scoped index")
            mapping = MappingScoped()
            map_types = MappingTypes().create()
            if vector_fields:
                map_types.add_vector(vector_fields, dims, similarity)
            if text_field:
                map_types.add_text(text_field)
            if metadata:
                map_types.add_metadata()
            mapping.types = MappingType(map_types).as_name(name)
        return cls(
            DocConfig.create(default),
            mapping,
        )

    @classmethod
    def from_dict(cls, json_data: dict):
        if not json_data:
            json_data = {}

        return cls(
            DocConfig.from_dict(json_data.get("doc_config", {})),
            MappingScoped.from_dict(json_data.get("mapping", {})),
        )


@attr.s
class DocConfig:
    docid_prefix_delim: Optional[str] = attr.ib(default="")
    docid_regexp: Optional[str] = attr.ib(default="")
    mode: Optional[str] = attr.ib(default="scope.collection.type_field")
    type_field: Optional[str] = attr.ib(default="type")

    @classmethod
    def create(cls, default=False):
        if default:
            mode = "type_field"
        else:
            mode = "scope.collection.type_field"
        return cls(
            "",
            "",
            mode,
            "type"
        )

    @classmethod
    def from_dict(cls, json_data: dict):
        if not json_data:
            json_data = {}

        return cls(
            json_data.get("docid_prefix_delim", ""),
            json_data.get("docid_regexp", ""),
            json_data.get("mode", "type_field"),
            json_data.get("type_field", "type"),
        )


@attr.s
class MetaDataProperty:
    dynamic: Optional[bool] = attr.ib()
    enabled: Optional[bool] = attr.ib()

    @classmethod
    def create(cls):
        return cls(
            True,
            True,
        )

    @property
    def as_dict(self):
        return self.__dict__


@attr.s
class Metadata:
    metadata: Optional[dict] = attr.ib()

    @classmethod
    def create(cls):
        return cls(
            MetaDataProperty(True, True).as_dict
        )

    @property
    def as_dict(self):
        return self.__dict__


@attr.s
class DefaultMappingScoped:
    dynamic: Optional[bool] = attr.ib()
    enabled: Optional[bool] = attr.ib()

    @classmethod
    def from_dict(cls, json_data: dict):
        if not json_data:
            json_data = {}

        return cls(
            json_data.get("dynamic", True),
            json_data.get("enabled", False),
        )


@attr.s
class DefaultMappingDefault:
    dynamic: Optional[bool] = attr.ib()
    enabled: Optional[bool] = attr.ib()
    properties: Optional[dict] = attr.ib()

    @classmethod
    def create(cls):
        return cls(
            True,
            True,
            {}
        )

    def add_vector(self, vector_fields: List[str], dims: List[int], similarity="dot_product"):
        for vector_field, dim in zip(vector_fields, dims):
            _property = VectorProperty().create(dim, vector_field, similarity).as_name(vector_field)
            self.properties.update(_property)

    def add_text(self, text_field="text"):
        _property = TextProperty().create(text_field).as_name(text_field)
        self.properties.update(_property)

    def add_metadata(self):
        _property = Metadata.create().as_dict
        self.properties.update(_property)

    @classmethod
    def from_dict(cls, json_data: dict):
        if not json_data:
            json_data = {}

        return cls(
            json_data.get("dynamic", True),
            json_data.get("enabled", False),
            json_data.get("properties", {}),
        )


@attr.s
class MappingScoped:
    default_analyzer: Optional[str] = attr.ib(default="standard")
    default_datetime_parser: Optional[str] = attr.ib(default="dateTimeOptional")
    default_field: [str] = attr.ib(default="_all")
    default_mapping: Optional[DefaultMappingScoped] = attr.ib(default=DefaultMappingScoped(True, False))
    default_type: Optional[str] = attr.ib(default="_default")
    docvalues_dynamic: Optional[bool] = attr.ib(default=False)
    index_dynamic: Optional[bool] = attr.ib(default=True)
    store_dynamic: Optional[bool] = attr.ib(default=False)
    type_field: Optional[str] = attr.ib(default="_type")
    types: Optional[MappingType] = attr.ib(default=None)

    @classmethod
    def from_dict(cls, json_data: dict):
        if not json_data:
            json_data = {}

        return cls(
            json_data.get("default_analyzer"),
            json_data.get("default_datetime_parser"),
            json_data.get("default_field"),
            DefaultMappingScoped.from_dict(json_data.get("default_mapping")),
            json_data.get("default_type"),
            json_data.get("docvalues_dynamic"),
            json_data.get("index_dynamic"),
            json_data.get("store_dynamic"),
            json_data.get("type_field"),
            None,
        )


@attr.s
class MappingDefault:
    default_analyzer: Optional[str] = attr.ib(default="standard")
    default_datetime_parser: Optional[str] = attr.ib(default="dateTimeOptional")
    default_field: [str] = attr.ib(default="_all")
    default_mapping: Optional[DefaultMappingDefault] = attr.ib(default=DefaultMappingDefault.create())
    default_type: Optional[str] = attr.ib(default="_default")
    docvalues_dynamic: Optional[bool] = attr.ib(default=False)
    index_dynamic: Optional[bool] = attr.ib(default=True)
    store_dynamic: Optional[bool] = attr.ib(default=True)
    type_field: Optional[str] = attr.ib(default="_type")

    @classmethod
    def from_dict(cls, json_data: dict):
        if not json_data:
            json_data = {}

        return cls(
            json_data.get("default_analyzer"),
            json_data.get("default_datetime_parser"),
            json_data.get("default_field"),
            DefaultMappingDefault.from_dict(json_data.get("default_mapping")),
            json_data.get("default_type"),
            json_data.get("docvalues_dynamic"),
            json_data.get("index_dynamic"),
            json_data.get("store_dynamic"),
            json_data.get("type_field"),
        )


@attr.s
class MappingType:
    struct: Optional[MappingTypes] = attr.ib()

    def as_name(self, name: str):
        response = {name: self.__dict__['struct']}
        return response


@attr.s
class MappingTypes:
    dynamic: Optional[bool] = attr.ib(default=False)
    enabled: Optional[bool] = attr.ib(default=True)
    properties: Optional[dict] = attr.ib(default=None)

    @classmethod
    def create(cls):
        return cls(
            False,
            True,
            {}
        )

    def add_vector(self, vector_fields: List[str], dims: List[int], similarity="dot_product"):
        for vector_field, dim in zip(vector_fields, dims):
            _property = VectorProperty().create(dim, vector_field, similarity).as_name(vector_field)
            self.properties.update(_property)

    def add_text(self, text_field="text"):
        _property = TextProperty().create(text_field).as_name(text_field)
        self.properties.update(_property)

    def add_metadata(self):
        _property = Metadata.create().as_dict
        self.properties.update(_property)

    @classmethod
    def from_dict(cls, json_data: dict):
        if not json_data:
            json_data = {}

        return cls(
            json_data.get("dynamic"),
            json_data.get("enabled"),
            json_data.get("properties"),
        )


@attr.s
class VectorProperty:
    vector_field: Optional[VectorField] = attr.ib(default=None)

    @classmethod
    def create(cls, dims=1536, vector_field="vector_field", similarity="dot_product"):
        field = VectorField()
        field.fields = [VectorFields(dims=dims, name=vector_field, similarity=similarity)]
        return cls(
            field
        )

    @classmethod
    def from_dict(cls, json_data: dict):
        if not json_data:
            json_data = {}

        return cls(
            VectorField.from_dict(json_data.get("vector_field")),
        )

    def as_name(self, name: str):
        response = {name: self.__dict__['vector_field']}
        return response


@attr.s
class TextProperty:
    text: Optional[TextField] = attr.ib(default=None)

    @classmethod
    def create(cls, text_field="text"):
        field = TextField()
        field.fields = [TextFields(name=text_field)]
        return cls(
            field
        )

    @classmethod
    def from_dict(cls, json_data: dict):
        if not json_data:
            json_data = {}

        return cls(
            TextField.from_dict(json_data.get("text")),
        )

    def as_name(self, name: str):
        response = {name: self.__dict__['text']}
        return response


@attr.s
class TextFields:
    index: Optional[bool] = attr.ib(default=True)
    name: Optional[str] = attr.ib(default="text")
    store: Optional[bool] = attr.ib(default=True)
    type: Optional[str] = attr.ib(default="text")

    @classmethod
    def create(cls, name="text"):
        return cls(
            True,
            name,
            True,
            "text"
        )

    @classmethod
    def from_dict(cls, json_data: dict):
        if not json_data:
            json_data = {}

        return cls(
            json_data.get("index"),
            json_data.get("name"),
            json_data.get("store"),
            json_data.get("type"),
        )


@attr.s
class VectorFields:
    dims: Optional[int] = attr.ib(default=1536)
    index: Optional[bool] = attr.ib(default=True)
    name: Optional[str] = attr.ib(default="vector_field")
    similarity: Optional[str] = attr.ib(default="dot_product")
    vector_index_optimized_for: Optional[str] = attr.ib(default="recall")
    type: Optional[str] = attr.ib(default="vector")

    @classmethod
    def create(cls, dims=1536, name="vector_field", similarity="dot_product"):
        return cls(
            dims,
            True,
            name,
            similarity,
            "recall",
            "vector"
        )

    @classmethod
    def from_dict(cls, json_data: dict):
        if not json_data:
            json_data = {}

        return cls(
            json_data.get("dims"),
            json_data.get("index"),
            json_data.get("name"),
            json_data.get("similarity"),
            json_data.get("store"),
            json_data.get("type"),
        )


@attr.s
class Store:
    index_type: Optional[str] = attr.ib(default="scorch")
    segment_version: Optional[int] = attr.ib(default=16)

    @classmethod
    def from_dict(cls, json_data: dict):
        if not json_data:
            json_data = {}

        return cls(
            json_data.get("indexType"),
            json_data.get("segmentVersion"),
        )


@attr.s
class VectorField:
    enabled: Optional[bool] = attr.ib(default=True)
    dynamic: Optional[bool] = attr.ib(default=False)
    fields: Optional[List[VectorFields]] = attr.ib(default=[])

    @classmethod
    def from_dict(cls, json_data: dict):
        if not json_data:
            json_data = {}

        return cls(
            json_data.get("enabled"),
            json_data.get("dynamic"),
            [VectorFields.from_dict(e) for e in json_data.get("fields", [])],
        )


@attr.s
class TextField:
    enabled: Optional[bool] = attr.ib(default=True)
    dynamic: Optional[bool] = attr.ib(default=False)
    fields: Optional[List[TextFields]] = attr.ib(default=[])

    @classmethod
    def from_dict(cls, json_data: dict):
        if not json_data:
            json_data = {}

        return cls(
            json_data.get("enabled"),
            json_data.get("dynamic"),
            [TextFields.from_dict(e) for e in json_data.get("fields", [])],
        )
