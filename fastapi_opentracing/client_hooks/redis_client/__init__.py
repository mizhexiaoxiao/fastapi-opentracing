def install_patch():
    redis_module = None
    
    # 尝试导入 redis.asyncio
    try:
        from redis import asyncio as aioredis
        redis_module = "redis"
    except ImportError:
        import aioredis
        redis_module = "aioredis"
    
    # 根据不同的模块和版本选择对应的 patch
    if redis_module == "aioredis" and aioredis.__version__.startswith("1"):
        from . import aioredis_low_level
        aioredis_low_level.install_patch()
    else:
        from . import aioredis_high_level
        aioredis_high_level.install_patch()