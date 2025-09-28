 for auth , i copy the repo from
  /Users/dengwei/work/ai/google_workspace_mcp/auth  to this repo
  ./auth . \n read the implemement from ./auth  , and usage is like
  @server.tool
  @require_google_service("drive", "drive_read")
  @handle_http_errors("list_docs_in_folder")
  async def list_docs_in_folder(
      service,
      ctx: Context,
      user_google_email: Optional[str] = None,
      folder_id: str = 'root',
      page_size: int = 100
  ): \n\n\n now  , i think it would better to replace this part: \n
  client = await self.get_client(user_id)
          spreadsheet = await client.open_by_key(spreadsheet_id)
  \n\n and add each function interactive with google sheet with the
  annotaion @require_google_service and add ctx: Context to the
  function . \n\n it was said that ctx will contains the header in
  auth for its our company omnimcp's standard.  \n is it corret to
  do so ?
  