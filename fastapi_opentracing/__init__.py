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


class TraceContext:
    """追踪上下文管理器，用于管理 span 的生命周期"""
    
    def __init__(self, operation_name=None):
        self.operation_name = operation_name or f"{project_name}.{namespace}_task"
        self.span = None
        self.scope = None
        self._trace_id = None
        self._parent_scope = None

    def __enter__(self):
        # 保存当前的 scope
        self._parent_scope = tracer.scope_manager.active
        parent_span = self._parent_scope.span if self._parent_scope else None
        if parent_span is not None:
            # 如果存在父 span，创建子 span
            self.span = tracer.start_span(
                operation_name=self.operation_name,
                child_of=parent_span
            )
        else:
            # 如果不存在父 span，创建根 span
            self.span = tracer.start_span(
                operation_name=f"{self.operation_name}_root"
            )
        
        # 只有在没有父 scope 时才激活新的 span
        if not self._parent_scope:
            self.scope = tracer.scope_manager.activate(self.span, True)

        self._trace_id = format(self.span.context.trace_id, 'x').zfill(32)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is not None:
                # 如果发生异常，记录到 span 中
                self.span.set_tag('error', True)
                self.span.log_kv({
                    'event': 'error',
                    'error.kind': exc_type.__name__,
                    'message': str(exc_val),
                })
        finally:
            if self.scope:
                self.scope.close()
            # 如果使用父scope,则需要手动finish span, 否则self.scope.close()会自动finish span
            if self._parent_scope:
                if self.span:
                    self.span.finish()

    @property
    def trace_id(self):
        """获取当前 trace_id"""
        if self.span:
            return format(self.span.context.trace_id, 'x').zfill(32)
        return None
    
    @property
    def span_id(self):
        """获取当前 span_id"""
        if self.span:
            return format(self.span.context.span_id, 'x').zfill(16)
        return None

# 使用示例（正常来说IO操作会自动记录span，这里是显示调用的代码示例）：
# 1. 在请求中使用:
# with TraceContext("http_request") as ctx:
#     logger.info(f"[trace_id={ctx.trace_id}] 开始处理请求")
#     # 调用其他服务
#     async with aiohttp.ClientSession() as session:
#         async with session.get("http://example.com") as resp:
#             data = await resp.json()
#     logger.info(f"[trace_id={ctx.trace_id}] 请求处理完成")

# 2. 在数据库操作中使用:
# with TraceContext("db_operation") as ctx:
#     logger.info(f"[trace_id={ctx.trace_id}] 开始数据库操作")
#     async with db_span(self, "SELECT * FROM users", "mysql") as span:
#         result = await User.filter(id=1).first()
#     logger.info(f"[trace_id={ctx.trace_id}] 数据库操作完成")

# 3. 在定时任务中使用:
# async def scheduled_task():
#     with TraceContext("scheduled_task") as ctx:
#         logger.info(f"[trace_id={ctx.trace_id}] 开始执行定时任务")
#         try:
#             await do_something()
#         except Exception as e:
#             logger.error(f"[trace_id={ctx.trace_id}] 任务执行失败: {str(e)}")
#             raise
#         logger.info(f"[trace_id={ctx.trace_id}] 定时任务执行完成")

# 4. 获取trace_id 和 span_id
# ctx = TraceContext("http_request")
# print(ctx.trace_id)
# print(ctx.span_id)
