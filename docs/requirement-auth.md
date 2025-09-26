
# reuse the user-id oauth from another repo
which share the auth info with given user-id using mongodb to query 

read the related code:
/Users/dengwei/work/ai/om3/api4/src/fastestai/tools/sheet/service.py

usage:
client = await self.base_service.get_client(user_id=user_id)
spreadsheet = await client.open_by_key(request.spreadsheet_id)
        
part of the implement :

async def get_client(self, user_id: str) -> AsyncioGspreadClient:
        user_credentials = await get_google_credentials(user_id)
        creds = Credentials(
            token=user_credentials.access_token,
            refresh_token=user_credentials.refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=os.getenv("GOOGLE_CLIENT_ID"),
            client_secret=os.getenv("GOOGLE_CLIENT_SECRET"),
            scopes=user_credentials.scope.split(),
        )
        async_client_manager = gspread_asyncio.AsyncioGspreadClientManager(
            lambda: creds
        )
        return await async_client_manager.authorize()



async def get_google_credentials(user_id: str) -> GoogleCredentials:
    collection = get_mongodb(db_name="play")[
        UserExternalAuthInfo.MongoConfig.collection
    ]
    doc = await collection.find_one(
        {
            "$and": [
                {"user_id": user_id},
                {"auth_info.scope": {"$regex": "spreadsheets", "$options": "i"}},
                {"auth_info.scope": {"$regex": "drive", "$options": "i"}},
            ]
        }
    )
    if not doc:
        raise UserError("Google credentials not found")
    auth_info = UserExternalAuthInfo.model_validate(doc)
    return GoogleCredentials(
        access_token=auth_info.auth_info["access_token"],
        refresh_token=auth_info.auth_info["refresh_token"],
        scope=auth_info.auth_info["scope"],
    )


# db 

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
    """
    if db_name not in DB_MAPPING:
        raise ValueError(f"Unknown database: {db_name}")

    config = DB_MAPPING[db_name]
    return AsyncIOMotorClient(config["uri"])[config["db"]]


BTW real oauth process will  will do it
  in the future 