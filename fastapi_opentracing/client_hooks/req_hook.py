<<<<<<< HEAD
import aiohttp.typedefs
from opentracing.ext import tags
from opentracing import Format
from fastapi_opentracing import tracer, get_current_span
from collections.abc import Mapping
=======
import aiohttp
from opentracing.ext import tags
from fastapi_opentracing import tracer, get_current_span
>>>>>>> 58047055971b82da42b16fc6e78724fcf1dacc0d

try:
    import aiohttp
except ImportError:
    pass
else:
    _aiohttp_client_session_request = aiohttp.ClientSession._request

async def request_wrapper(self, method: str, url: str, **kwargs):
    """
    Wrapper for aiohttp client requests to add tracing
    """
    span = await get_current_span()
    if span is None:
<<<<<<< HEAD
        req_span = tracer.start_span(operation_name=f"HTTP {method}")
    else:
        req_span = tracer.start_span(operation_name=f"HTTP {method}", child_of=span)
    
    # 创建HTTP请求的span_tags
    req_span_tags = {
=======
        return await _aiohttp_client_session_request(self, method, url, **kwargs)

    # 创建HTTP请求的span
    span_tags = {
>>>>>>> 58047055971b82da42b16fc6e78724fcf1dacc0d
        tags.SPAN_KIND: tags.SPAN_KIND_RPC_CLIENT,
        tags.HTTP_METHOD: method,
        tags.HTTP_URL: url,
        "component": "aiohttp.client"
    }
<<<<<<< HEAD
    carrier = {}
    tracer.inject(span_context=req_span, format=Format.HTTP_HEADERS, carrier=carrier)
    # 添加请求头信息到span_tags
    if 'headers' in kwargs:
        headers = kwargs['headers']
        if headers and isinstance(headers, (dict, Mapping)):
            if "X-B3-SpanId" in headers:
                headers["X-B3-SpanId"] = carrier["X-B3-SpanId"]
            if "X-B3-ParentSpanId" in headers:
                headers["X-B3-ParentSpanId"] = carrier["X-B3-ParentSpanId"]
            for header_name, header_value in headers.items():
                header_lower = header_name.lower()
                req_span_tags[f"http.request.header.{header_lower}"] = str(header_value)
            kwargs['headers'] = headers
    for key, value in req_span_tags.items():
        req_span.set_tag(key, value)

    return await _handle_request(self, method, url, kwargs, req_span)
    
async def _handle_request(self, method, url, kwargs, req_span):
    """Helper function to handle the actual request and span operations"""
    try:
        response = await _aiohttp_client_session_request(self, method, url, **kwargs)
        # 添加响应信息到span
        req_span.set_tag(tags.HTTP_STATUS_CODE, response.status)
        if response.status >= 400:
            req_span.set_tag(tags.ERROR, True)
        return response     
    except Exception as e:
        # 记录异常信息
        req_span.set_tag(tags.ERROR, True)
        req_span.log_kv({
            "event": "error",
            "error.kind": type(e).__name__,
            "error.message": str(e)
        })
        raise
    finally:
        req_span.finish()
=======

    with tracer.start_span(
        operation_name=f"HTTP {method}",
        child_of=span,
        tags=span_tags
    ) as request_span:
        try:
            response = await _aiohttp_client_session_request(self, method, url, **kwargs)
            
            # 添加响应信息到span
            request_span.set_tag(tags.HTTP_STATUS_CODE, response.status)
            if response.status >= 400:
                request_span.set_tag(tags.ERROR, True)
                
            return response
            
        except Exception as e:
            # 记录异常信息
            request_span.set_tag(tags.ERROR, True)
            request_span.log_kv({
                "event": "error",
                "error.kind": type(e).__name__,
                "error.message": str(e)
            })
            raise
>>>>>>> 58047055971b82da42b16fc6e78724fcf1dacc0d

def install_patch():
    """Install the request patch"""
    if "_aiohttp_client_session_request" not in globals():
        raise Exception("aiohttp patch install failed")
    aiohttp.ClientSession._request = request_wrapper
