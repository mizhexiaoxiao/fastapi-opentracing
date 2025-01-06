import aiohttp
from opentracing.ext import tags
from fastapi_opentracing import tracer, get_current_span

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
        return await _aiohttp_client_session_request(self, method, url, **kwargs)

    # 创建HTTP请求的span
    span_tags = {
        tags.SPAN_KIND: tags.SPAN_KIND_RPC_CLIENT,
        tags.HTTP_METHOD: method,
        tags.HTTP_URL: url,
        "component": "aiohttp.client"
    }

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

def install_patch():
    """Install the request patch"""
    if "_aiohttp_client_session_request" not in globals():
        raise Exception("aiohttp patch install failed")
    aiohttp.ClientSession._request = request_wrapper
