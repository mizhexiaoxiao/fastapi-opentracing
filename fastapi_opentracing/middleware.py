import typing
from opentracing.ext import tags
from opentracing.propagation import Format
from starlette.datastructures import MutableHeaders
import functools
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp, Receive, Scope, Send, Message
from typing import Union

from . import tracer

RequestResponseEndpoint = typing.Callable[
    [Request], typing.Awaitable[Response]
]
DispatchFunction = typing.Callable[
    [Request, RequestResponseEndpoint], typing.Awaitable[Response]
]

class SimpleBaseMiddleware:

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive=receive)
        send = functools.partial(self.send, send=send, request=request)

        response = await self.before_request(request) or self.app
        await response(request.scope, request.receive, send)
        await self.after_request(request)

    async def get_body(self, request):
        """获取请求BODY"""
        async def _receive():
            return {"type": "http.request", "body": body}

        body = await request.body()
        request._receive = _receive
        return body

    async def before_request(self, request: Request) -> Union[Response, None]:
        """如果需要修改请求信息，可直接重写此方法"""
        return self.app

    async def after_request(self, request: Request):
        """请求后的处理【记录请求耗时等，注意这里没办法对响应结果进行处理】"""
        return None

    async def send(self, message: Message, send: Send, request: Request) -> None:
        """重写send方法【不重写则默认使用原来的】"""
        await send(message)

    async def update_request_header(self, request, key, value):
        """更新请求头"""
        key, value = str(key).encode(), str(value).encode()
        for index, item in enumerate(request.scope['headers']):
            if item[0] == key:
                request.scope['headers'][index] = (key, value)
                return

        request.scope['headers'].append((key, value))


class OpenTracingMiddleware(SimpleBaseMiddleware):
    _extra_headers = [
        # All applications should propagate x-request-id. This header is
        # included in access log statements and is used for consistent trace
        # sampling and log sampling decisions in Istio.
        "x-request-id",
        # Lightstep tracing header. Propagate this if you use lightstep tracing
        # in Istio (see
        # https://istio.io/latest/docs/tasks/observability/distributed-tracing/lightstep/)
        # Note: this should probably be changed to use B3 or W3C TRACE_CONTEXT.
        # Lightstep recommends using B3 or TRACE_CONTEXT and most application
        # libraries from lightstep do not support x-ot-span-context.
        "x-ot-span-context",
        # Datadog tracing header. Propagate these headers if you use Datadog
        # tracing.
        "x-datadog-trace-id",
        "x-datadog-parent-id",
        "x-datadog-sampling-priority",
        # W3C Trace Context. Compatible with OpenCensusAgent and Stackdriver
        # Istio configurations.
        "traceparent",
        "tracestate",
        # Cloud trace context. Compatible with OpenCensusAgent and Stackdriver
        # Istio configurations.
        "x-cloud-trace-context",
        # Grpc binary trace context. Compatible with OpenCensusAgent nad
        # Stackdriver Istio configurations.
        "grpc-trace-bin",
        # b3 trace headers. Compatible with Zipkin, OpenCensusAgent, and
        # Stackdriver Istio configurations. Commented out since they are
        # propagated by the OpenTracing tracer above.
        # 'x-b3-traceid',
        # 'x-b3-spanid',
        # 'x-b3-parentspanid',
        # 'x-b3-sampled',
        # 'x-b3-flags',
        # Application-specific headers to forward.
        "user-agent",
        "x-weike-node",
        "x-weike-forward",
    ]

    async def before_request(self, request: Request) -> Union[Response, None]:
        try:
            # Create a new span context, reading in values (traceid,
            # spanid, etc) from the incoming x-b3-*** headers.
            span_ctx = tracer.extract(
                Format.HTTP_HEADERS, dict(request.headers)
            )
            # Note: this tag means that the span will *not* be
            # a child span. It will use the incoming traceid and
            # spanid. We do this to propagate the headers verbatim.
            rpc_tag = {tags.SPAN_KIND: tags.SPAN_KIND_RPC_SERVER}

            # application tag
            rpc_tag['framework'] = 'Fastapi'
            rpc_tag['uri'] = request.url._url.split("?")[0]
            rpc_tag['method'] = request.method
            rpc_tag['query_params'] = str(request.query_params)
            rpc_tag["client_ip"] = request.client.host if request.client else None
            rpc_tag["user_agent"] = request.headers.get('user-agent'),
            rpc_tag["host"] = request.headers.get("host")
            rpc_tag["route"] = request.url.path
            # force sampling policy for tailsamplingprocessor
            force_trace = request.headers.get("x-weike-force-trace")
            if force_trace:
                rpc_tag['x-weike-force-trace'] = "yes"

            span = tracer.start_span(
                operation_name="opentracing-middleware",
                child_of=span_ctx,
                tags=rpc_tag,
            )
        except Exception as e:
            # We failed to create a context, possibly due to no
            # incoming x-b3-*** headers. Start a fresh span.
            # Note: This is a fallback only, and will create fresh headers,
            # not propagate headers.
            span = tracer.start_span("opentracing-middleware-new-trace")
            span.log_event("error", repr(e))
            span.log_kv({"message": "Failed to extract span context"})
            span.log_kv({"stack": dict(request.headers)})
            # Keep this in sync with the headers in details and reviews.
        extra_headers = {}

        for ihdr in self._extra_headers:
            val = request.headers.get(ihdr)
            if val is not None:
                extra_headers[ihdr] = val
        tracer.scope_manager.activate(span, True)
        setattr(span, "extra_headers", extra_headers)

    async def after_request(self, request: Request):
        tracer.scope_manager.active.close()

    async def send(self, message, send, request):
        if message["type"] != "http.response.start":
            await send(message)
            return
        headers = MutableHeaders(scope=message)
        req_headers = {k.lower(): v for k, v in dict(request.headers).items()}
        headers.append("x-b3-traceid", req_headers.get("x-b3-traceid", ""))
        headers.append("x-b3-sampled", req_headers.get("x-b3-sampled", ""))
        await send(message)
