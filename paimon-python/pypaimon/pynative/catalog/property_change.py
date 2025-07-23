from abc import ABC
from typing import List, Tuple, Dict, Set


class PropertyChange(ABC):
    @staticmethod
    def set_property(property: str, value: str) -> "PropertyChange":
        return SetProperty(property, value)

    @staticmethod
    def remove_property(property: str) -> "PropertyChange":
        return RemoveProperty(property)

    @staticmethod
    def get_set_properties_to_remove_keys(changes: List["PropertyChange"]) -> Tuple[Dict[str, str], Set[str]]:
        set_properties: Dict[str, str] = {}
        remove_keys: Set[str] = set()

        for change in changes:
            if isinstance(change, SetProperty):
                set_properties[change.property] = change.value
            elif isinstance(change, RemoveProperty):
                remove_keys.add(change.property)

        return set_properties, remove_keys


class SetProperty(PropertyChange):
    def __init__(self, property: str, value: str):
        self.property = property
        self.value = value


class RemoveProperty(PropertyChange):
    def __init__(self, property: str):
        self.property = property
