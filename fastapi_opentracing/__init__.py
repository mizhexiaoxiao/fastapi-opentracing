__name__ = "fastapi_opentracing"
from opentracing.scope_managers.contextvars import ContextVarsScopeManager
from opentracing.propagation import Format
from jaeger_client import Config
import os
from functools import lru_cache

# deployment.yaml will set the following env for each service
project_name = os.getenv("PROJECT_NAME", "PROJECT_NAME")
namespace = os.getenv("NAMESPACE", "NAMESPACE")

config = Config(
    config={
        "sampler": {"type": "const", "param": 1},
        "logging": False,
        "reporter_queue_size": 2000,
        "propagation": "b3",  # Compatible with istio
        "generate_128bit_trace_id": True,  # Compatible with istio
        # Initializing the tracer will execute the gethostbyname function, 
        # which is very time-consuming. Set the host here to avoid searching
        "tags": {"ip": "127.0.0.1"} 
    },
    service_name=f"{project_name}.{namespace}",
    scope_manager=ContextVarsScopeManager(),
    validate=True,
)

tracer = config.initialize_tracer()

def create_tracer(service_name):
    config = Config(
        config={
            "sampler": {"type": "const", "param": 1},
            "logging": False,
            "reporter_queue_size": 2000,
            "propagation": "b3",  # Compatible with istio
            "generate_128bit_trace_id": True,  # Compatible with istio
            "tags": {"ip": "127.0.0.1"} 
        },
        service_name=service_name,
        scope_manager=ContextVarsScopeManager(),
        validate=True,
    )   
    return config.new_tracer()


@lru_cache(maxsize=100)
def get_tracer(service_name):
    return create_tracer(service_name)


async def get_opentracing_span_headers():
    scope = tracer.scope_manager.active
    carrier = {}
    if scope is not None:
        span = scope.span
        tracer.inject(
            span_context=span, format=Format.HTTP_HEADERS, carrier=carrier
        )
        for k, v in getattr(span, "extra_headers", {}).items():
            carrier[k] = v
    return carrier


async def get_current_span():
    scope = tracer.scope_manager.active
    if scope is not None:
        return scope.span
    return None


def sync_get_current_span():
    scope = tracer.scope_manager.active
    if scope is not None:
        return scope.span
    return None
