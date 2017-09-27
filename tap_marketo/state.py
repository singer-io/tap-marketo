class State:
    def __init__(self, bookmarks=None, current_stream=None, use_corona=True, default_start_date=None):
        self.bookmarks = bookmarks
        self.current_stream = current_stream
        self.use_corona = use_corona
        self.default_start_date = default_start_date

    @classmethod
    def from_dict(cls, state):
        return cls(state.get("bookmarks"), state.get("current_stream"))

    def to_dict(self):
        rtn = {"bookmarks": self.bookmarks}
        if self.current_stream:
            rtn["current_stream"] = self.current_stream

        return rtn

    def get_entity_state(self, entity):
        if entity.name not in self.bookmarks:
            self.bookmarks[entity.name] = {}

        return self.bookmarks[entity.name]

    def get_bookmark(self, entity):
        if not entity.replication_key:
            raise Exception("Entities without replication keys do not support bookmarking")

        return self.get_entity_state(entity).get(entity.replication_key, self.default_start_date)

    def set_bookmark(self, entity, bookmark):
        if not entity.replication_key:
            raise Exception("Entities without replication keys do not support bookmarking")

        if bookmark > self.get_bookmark(entity):
            self.get_entity_state(entity)[entity.replication_key] = bookmark

    def get_export_id(self, entity):
        return self.get_entity_state(entity).get("export_id")

    def set_export_id(self, entity, export_id):
        if export_id is None:
            self.get_entity_state(entity).pop("export_id")
        else:
            self.get_entity_state(entity)["export_id"] = export_id
