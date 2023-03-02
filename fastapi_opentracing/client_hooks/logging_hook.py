import logging
from fastapi_opentracing import sync_get_current_span, tracer
_logging_log = logging.Logger.log
from opentracing import Span

ERROR = "ERROR"
WARNING = "WARNING"
INFO = "INFO"
DEBUG = "DEBUG"

error_level = [ERROR]

def _logging_wrapper(self, level, msg, *args, **kwargs):

    child_span = None
    level_name = logging._levelToName.get(level)
    if level_name in error_level:
        parent_span = sync_get_current_span()
        if parent_span is not None:
            child_span: Span = tracer.start_span(
                operation_name=level_name, 
                child_of=parent_span,
                tags={"error": "yes"}
            )
            child_span.log_kv({"event": "error", "message": msg, "level": level_name})

    _logging_log(self, level, msg, *args, **kwargs)    

    if child_span is not None:
        child_span.finish()

def install_patch():
    logging.Logger.log = _logging_wrapper