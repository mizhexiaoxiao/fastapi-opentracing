from __future__ import absolute_import

import opentracing
from opentracing.ext import tags
from fastapi_opentracing import tracer, get_current_span, get_tracer
from ._const import TRANS_TAGS


class Context:
    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class SpanContext:
    def __init__(self, operation_span, endpoint_span):
        self.operation_span = operation_span
        self.endpoint_span = endpoint_span

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.endpoint_span.finish()
        self.operation_span.finish()
    
    def finish(self):
        self.endpoint_span.finish()
        self.operation_span.finish()


async def db_span(self, query: str, db_instance, db_type="SQL"):
    """
    Span for database
    """
    span = await get_current_span()
    if span is None:
        return Context()
    statement = query.strip()
    spance_idx = statement.find(" ")
    if query in TRANS_TAGS:
        operation = query
    else:
        if spance_idx == -1:
            operation = " "
        else:
            operation = statement[0:spance_idx]

    span_tag = {tags.SPAN_KIND: tags.SPAN_KIND_RPC_CLIENT}
    span_tag[tags.DATABASE_STATEMENT] = statement
    span_tag[tags.DATABASE_TYPE] = db_type
    span_tag[tags.DATABASE_INSTANCE] = db_instance
    span_tag[tags.DATABASE_USER] = (
        self.user if hasattr(self, "user") else self._parent.user
    )
    host = self.host if hasattr(self, "host") else self._parent.host
    port = self.port if hasattr(self, "port") else self._parent.port
    database = (
        self.database if hasattr(self, "database") else self._parent.database
    )
    peer_address = f"{db_instance}://{host}:{port}/{database}"
    span_tag[tags.PEER_ADDRESS] = peer_address

    db_span = start_child_span(
        operation_name=operation, tracer=tracer, parent=span, span_tag=span_tag
    )
    
    # 添加应用端点span
    endpoint_span_tag = {
        tags.SPAN_KIND: tags.SPAN_KIND_RPC_SERVER,
        "application": db_instance,
        "component": db_type,
        tags.DATABASE_INSTANCE: db_instance,
        tags.PEER_ADDRESS: span_tag[tags.PEER_ADDRESS]
    }

    database_tracer = get_tracer(peer_address)
    endpoint_span = database_tracer.start_span(
        operation_name=f"{db_instance}_endpoint",
        child_of=db_span,
        tags=endpoint_span_tag
    )
    
    return SpanContext(db_span, endpoint_span)


def redis_span(self, span, operation, statement, db_instance, db_type="redis"):
    """
    Span for redis
    """
    span_tag = {tags.SPAN_KIND: tags.SPAN_KIND_RPC_CLIENT}
    span_tag[tags.DATABASE_STATEMENT] = statement
    span_tag[tags.DATABASE_TYPE] = db_type
    span_tag[tags.DATABASE_INSTANCE] = db_instance

    self._statement = " "

    host, port = (
        self._pool_or_conn.address
        if hasattr(self._pool_or_conn, "address")
        else (" ", " ")
    )
    db = self._pool_or_conn.db if hasattr(self._pool_or_conn, "db") else " "
    minsize = (
        self._pool_or_conn.minsize
        if hasattr(self._pool_or_conn, "minsize")
        else " "
    )
    maxsize = (
        self._pool_or_conn.maxsize
        if hasattr(self._pool_or_conn, "maxsize")
        else " "
    )
    peer_address = f"redis://:{host}:{port}/{db}"
    span_tag[tags.PEER_ADDRESS] = peer_address
    span_tag["redis.minsize"] = minsize
    span_tag["redis.maxsize"] = maxsize

    redis_span = start_child_span(
        operation_name=operation, tracer=tracer, parent=span, span_tag=span_tag
    )
    
    # 添加Redis端点span
    endpoint_span_tag = {
        "application": db_instance,
        "component": db_type,
        tags.DATABASE_INSTANCE: db_instance,
        tags.PEER_ADDRESS: peer_address,
        "redis.minsize": span_tag["redis.minsize"],
        "redis.maxsize": span_tag["redis.maxsize"]
    }
    redis_tracer = get_tracer(peer_address)
    endpoint_span = redis_tracer.start_span(
        operation_name=f"{db_instance}_endpoint",
        child_of=redis_span,
        tags=endpoint_span_tag
    )

    return SpanContext(redis_span, endpoint_span)


async def redis_span_high_level(
    self, span, operation, statement, db_instance, db_type="redis"
):
    """
    Span for redis high level
    """
    span_tag = {tags.SPAN_KIND: tags.SPAN_KIND_RPC_CLIENT}
    span_tag[tags.DATABASE_STATEMENT] = statement
    span_tag[tags.DATABASE_TYPE] = db_type
    span_tag[tags.DATABASE_INSTANCE] = db_instance

    conn_kwargs = self.connection_pool.connection_kwargs
    db = conn_kwargs.get("db", 0)
    host = conn_kwargs.get("host", "localhost")
    port = conn_kwargs.get("port", 6379)
    max_connections = conn_kwargs.get("max_connections", "")

    peer_address = f"redis://:{host}:{port}/{db}"
    span_tag[tags.PEER_ADDRESS] = peer_address
    span_tag["redis.maxsize"] = max_connections

    redis_span = start_child_span(
        operation_name=operation, tracer=tracer, parent=span, span_tag=span_tag
    )
    
    # 添加Redis端点span
    endpoint_span_tag = {
        tags.SPAN_KIND: tags.SPAN_KIND_RPC_SERVER,
        "application": db_instance,
        "component": db_type,
        tags.DATABASE_INSTANCE: db_instance,
        tags.PEER_ADDRESS: peer_address,
        "redis.maxsize": span_tag["redis.maxsize"]
    }
    redis_tracer = get_tracer(peer_address)
    endpoint_span = redis_tracer.start_span(
        operation_name=f"{db_instance}_endpoint",
        child_of=redis_span,
        tags=endpoint_span_tag
    )
    
    return SpanContext(redis_span, endpoint_span)


def start_child_span(
    operation_name: str, tracer=None, parent=None, span_tag=None
):
    """
    Start a new span as a child of parent_span. If parent_span is None,
    start a new root span.
    :param operation_name: operation name
    :param tracer: Tracer or None (defaults to opentracing.tracer)
    :param parent: parent or None
    :param span_tag: optional tags
    :return: new span
    """
    tracer = tracer or opentracing.tracer
    return tracer.start_span(
        operation_name=operation_name, child_of=parent, tags=span_tag
    )
