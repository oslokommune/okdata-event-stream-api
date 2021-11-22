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
        self.num_records = len(records)
        super().__init__(f"Request failed for some elements: {records}")
