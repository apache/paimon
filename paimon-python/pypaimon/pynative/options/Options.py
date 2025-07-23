class Options:
    def __init__(self, map):
        self.data = map

    @classmethod
    def from_none(cls):
        return cls({})

    def to_map(self) -> dict:
        return self.data

    def get(self, key: str, default=None):
        return self.data.get(key, default)

    def set(self, key: str, value):
        self.data[key] = value
