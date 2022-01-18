import os

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from okdata.aws.logging import add_fastapi_logging

from resources import stream, sinks, subscribable, events, events_statistics
from resources.errors import ErrorResponse


root_path = os.environ.get("ROOT_PATH", "")
app = FastAPI(title="event-stream-api", version="0.1.0", root_path=root_path)

add_fastapi_logging(app)

prefix = "/{dataset_id}/{version}"

app.include_router(stream.router, prefix=prefix, tags=["stream"])
app.include_router(sinks.router, prefix=prefix + "/sinks", tags=["sinks"])
app.include_router(
    subscribable.router,
    prefix=prefix + "/subscribable",
    tags=["subscribable"],
)
app.include_router(
    events.router,
    prefix=prefix + "/events",
    tags=["events"],
)
app.include_router(
    events_statistics.router,
    prefix=prefix + "/events/statistics",
    tags=["event statistics"],
)


@app.exception_handler(ErrorResponse)
def abort_exception_handler(request: Request, exc: ErrorResponse):
    return JSONResponse(
        status_code=exc.status_code,
        content={"message": exc.message, **exc.extra_context},
    )
