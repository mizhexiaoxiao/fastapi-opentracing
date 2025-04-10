import logging
from fastapi_opentracing import sync_get_current_span, tracer
_logging_log = logging.Logger._log
from opentracing import Span

ERROR = "ERROR"
WARNING = "WARNING"
INFO = "INFO"
DEBUG = "DEBUG"

error_level = [ERROR]

def _logging_wrapper(self, level, msg, args, exc_info=None, extra=None, stack_info=False, stacklevel=1):
    try:
        child_span = None
        level_name = logging._levelToName.get(level)
        if level_name in error_level:
            span = sync_get_current_span()
            if span is not None:
                child_span: Span = tracer.start_span(
                    operation_name=level_name, 
                    child_of=span,
                    tags={"error": "yes"}
                )
                child_span.log_kv({"event": "error", "message": msg, "level": level_name})

        _logging_log(self, level, msg, args, exc_info, extra, stack_info, stacklevel)

        if child_span is not None:
            child_span.finish()
    except Exception as e:
        print(e)

def install_patch():
    logging.Logger._log = _logging_wrapper