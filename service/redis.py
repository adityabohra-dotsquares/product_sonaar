import os
import redis.asyncio as redis
from dotenv import load_dotenv

load_dotenv()


def create_redis_client() -> redis.Redis:
    return redis.Redis(
        host=os.getenv("REDIS_HOST"),
        port=int(os.getenv("REDIS_PORT")),
        socket_connect_timeout=10,
        decode_responses=True,
    )


def get_redis_url(db: int = 0) -> str:
    SERVER_MODE = os.getenv("SERVER_MODE", "development").lower()

    if SERVER_MODE != "development":
        # Cloud Run / Production Redis
        host = os.getenv("REDIS_HOST", "10.192.0.2")
        port = int(os.getenv("REDIS_PORT", 6379))
        print("this is server mode", SERVER_MODE)
        print("we are in production", host, port)
        return f"redis://{host}:{port}/{db}"
    else:
        # Local Docker Redis
        print("we are in development mode", SERVER_MODE)
        REDIS_HOST = os.getenv("REDIS_HOST")
        REDIS_PORT = os.getenv("REDIS_PORT")
        return f"redis://{REDIS_HOST}:{REDIS_PORT}/{db}"
