"""
Factory module for DataTable MCP - MongoDB connections
Based on the shared authentication approach from requirement-auth.md
"""

import os
from typing import Dict, Any, Mapping
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

# 数据库配置映射
DB_MAPPING: Dict[str, Dict[str, str]] = {
    "play": {
        "uri": os.getenv("PLAY__MONGO_URI", "mongodb://localhost:27017"),
        "db": os.getenv("PLAY__MONGO_DB", "playground"),
    },
}


def get_mongodb(db_name: str) -> AsyncIOMotorDatabase[Mapping[str, Any]]:
    """
    根据数据库名称获取对应的 MongoDB 连接
    Based on the original factory implementation
    """
    if db_name not in DB_MAPPING:
        raise ValueError(f"Unknown database: {db_name}")

    config = DB_MAPPING[db_name]
    return AsyncIOMotorClient(config["uri"])[config["db"]]