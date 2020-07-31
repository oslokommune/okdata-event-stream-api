class ResourceConflict(Exception):
    pass


class ParentResourceNotReady(ResourceConflict):
    pass


class ResourceNotFound(Exception):
    pass
