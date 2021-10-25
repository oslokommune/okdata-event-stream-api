import time

from okdata.aws.logging import _init_logger
from starlette.middleware.base import BaseHTTPMiddleware

_start_time = None
_exception = None


def _logging_middleware(request, call_next):
    async def handler(event, context):
        global _exception

        _exception = None
        _init_logger(handler, event, context)
        try:
            return await call_next(request)
        except Exception as e:
            _exception = e
            raise e

    return handler(request.scope.get("aws.event", {}), request.scope.get("aws.context"))


def add_logging_middleware(app):
    app.add_middleware(BaseHTTPMiddleware, dispatch=_logging_middleware)

    @app.on_event("startup")
    async def startup_event():
        global _start_time
        _start_time = time.perf_counter_ns()

    @app.on_event("shutdown")
    async def shutdown_event():
        from okdata.aws.logging import _logger

        if _exception:
            _logger = _logger.bind(exc_info=e, level="error")

        duration_ms = (time.perf_counter_ns() - _start_time) / 1000000.0
        _logger.msg("", duration_ms=duration_ms)
