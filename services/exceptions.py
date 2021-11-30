class ResourceConflict(Exception):
    pass


class ParentResourceNotReady(ResourceConflict):
    pass


class ResourceNotFound(Exception):
    pass


class SubResourceNotFound(Exception):
    pass


class ResourceUnderConstruction(Exception):
    pass


class ResourceUnderDeletion(Exception):
    pass


class PutRecordsError(Exception):
    def __init__(self, records):
        self.records = records
        super().__init__(
            "Request failed for {} element{}".format(
                len(records),
                "s" if len(records) > 1 else "",
            )
        )
