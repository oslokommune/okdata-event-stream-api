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


class PostEventsError(Exception):
    pass


class PostEventsFailedElements(Exception):
    def __init__(self, failed_elements):
        self.failed_elements = failed_elements
    pass