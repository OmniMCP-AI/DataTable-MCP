"""
Models for external authentication info
Based on the shared authentication approach
"""

from typing import Dict, Any
from pydantic import BaseModel


class UserExternalAuthInfo(BaseModel):
    """Model for storing external authentication information"""
    user_id: str
    auth_info: Dict[str, Any]

    class MongoConfig:
        collection = "user_external_auth_info"


class GoogleCredentials(BaseModel):
    """Google credentials model (same as original service)"""
    access_token: str
    refresh_token: str
    scope: str