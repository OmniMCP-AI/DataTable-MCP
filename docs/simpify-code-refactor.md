# requirement and situation

currently this repo is over-complex

ideal architecure 
--------------------------------------------
| wrapper of mcp                           | 
|  | standard datatable api                |
|   | package of google sheet high level | |
|       | gspread-asyncio                | |
--------------------------------------------

would be better  google sheet high level  inherrit Standard DataTable API
(Standard DataTable API )
|
|
(google sheet high level )


package of google sheet high level
is in : third_party/google_sheets
future other excel will still follow this datatable api standard
and  excel will in third_party/excel


this mcp need to focus on a standard datatable tool  :
  - write_new_sheet
  - append_rows
  - append_columns
  - update_range
  - load_data_table


## my improve plan

### stage1
  remove unnessisary tool  mcp.tool, only keep the following

@mcp.tool
async def write_new_sheet
@mcp.tool
async def append_rows
@mcp.tool
async def append_columns
@mcp.tool
async def update_range 
@mcp.tool
async def load_data_table   

  clean up all other to temp/old_code

### stage2

continue to remove some of the unneeded code:
- since we dont need the in memory dataframe, so @talbe_manager.py most of them are unneeded
- class DataTable only keep the interface not any implement , keep align with stage 1 mcp tool 
 - such as DataTable should had a interface of update_range but  implement on a google_sheet class in  current detailed_tools.py. this class inheri DataTable 

some other interface implement in google_sheet class has their implement  write_new_sheet/append_rows/append_columns/update_range/load_data_table 
others just inferface only


### stage3
  remove unused test code  

## important on every stage refactor
most important test case , must not break this test case:
- test_mcp_client_calltool.py 
 this file require start main.py


### stage4

make the fast mcp Context keep only in wrapper of mcp , instead of the the standard datatable API 
so that i could migrate this core standard datatable API without require fastmcp, then i could allow update_range with data support dataframe. 
but since fastmcp still has limitation that could not support dataframe, only support at data: list[list[int | str | float | bool | None or dict]]

here's my previouly thought but after a series of code changed, it break the system for oauth problem. draw out a plan how to acheieve my goal:
- to support datframe at standardable
- not use fastmcp for oauth (might)

---------------------------------------------stage4 previous thought---------------------------------------------------------------------------------

so i'd love to make the following:
```
@mcp.tool
async def update_range(
    ctx: Context,
    uri: str = Field(
        description="Google Sheets URI. Supports full URL pattern (https://docs.google.com/spreadsheets/d/{spreadsheetID}/edit?gid={gid})"
    ),
    data: list[list[int | str | float | bool | None]] = Field(
        description="2D array of cell values (rows × columns). CRITICAL: Must be a nested list/array structure [[row1_col1, row1_col2], [row2_col1, row2_col2]], NOT a string. Each inner list represents one row. Accepts int, str, float, bool, or None values."
    ),
    range_address: str = Field(
        description="Range in A1 notation. Examples: single cell 'B5', row range 'A1:E1', column range 'B:B' or 'B1:B10', 2D range 'A1:C3'. Range auto-expands if data dimensions exceed specified range."
    )
) -> Dict[str, Any]:
    google_sheet = GoogleSheetDataTable()
    return await google_sheet.update_range(ctx, uri, data, range_address)
```
to
```
@mcp.tool

async def update_range(
    ctx: Context,
    ...
) -> Dict[str, Any]:
    google_sheet = GoogleSheetDataTable()
    return await google_sheet.update_range(uri, data, range_address)
```

so that keep the third-party clean without import fastmcp

make the same for other @mcp.tool

even more is it  @require_google_service("sheets", "sheets_read") better to add this @require_google_service together with @mcp.tool and remove them from the thirdpary
eg:
```
@mcp.tool
@require_google_service(xx, yy) # not nessiary
async def update_range(
    ctx: Context,
    ...
) -> Dict[str, Any]:
    google_sheet = GoogleSheetDataTable()
    return await google_sheet.update_range(uri, data, range_address)
```

try to keep ctx out of third-party

------------------------------------------------stage4 previous thought----------------------------------------------------------------
#### stage4.1

give a cleaner version 

@server.tool
@require_google_service("sheets", "sheets_write")
@handle_http_errors("modify_sheet_values")
async def modify_sheet_values(
    service,
    ctx: Context,
    spreadsheet_id: str,
    range_name: str,
    user_google_email: Optional[str] = None,
    values: Optional[List[List[str]]] = None,
    value_input_option: str = "USER_ENTERED",
    clear_values: bool = False,
):
    
   """
    impelment body without ctx
   """ 
   pass 

#### previous seesion history 

i mean the  @require_google_service + @mcp.tool look good to me. but you revert changed this to my very original version . you  privouly modify seems good, but need to
  improve , which let too many implement code in detailed_tools.py . i wanna it to write the implement logic to GoogleSheetDataTable. but it seems
  GoogleSheetDataTable.update_range(service, ctx ) which contains the injeced service , if it really nessisary leave it there , but ctx seems need to be removed. revise the
  plan again , only execute after my confirmation.  2) dont understimate my orginal improvement at main branch , which i give a copy at
  /Users/dengwei/work/ai/om4/DataTable-MCP-main/datatable_tools/detailed_tools.py , i handle quite a few exception , make it more convience and more tool call success rate
  high . try to maintain the original logic 

#### stage4.3 

support dataframe update_range/append_rows/append_columns


### stage5 

#### stage 5.1

 added @datatable_tools/tools/mcpplus.py
 added @tests/tools/test_mcpplus.py

these are files from another repo ,   suppose to run in another repo to call mcp tool using MCP protocol (sse or streamablehttp) .  i move it to this repo

##### requirement for this stage 

now  i wanna testing call from by src code

to support the follow usage
```
result = await call_tool_by_sse(
            sse_url=TEST_SSE_URL,
            tool_name="google_sheets__update_range",
            direct_call = true,
            args={
                'uri': TWITTER_CASE_URI,
                'data': df_test,
                'range_address': 'A2:D3'
            }
        )
```

if direct_call is false, continue the orininal logic .
when direct_call is true, mean call the core standard API to 

my plan is :

 - update_range  will direct call the following in mcp_tools.py 
```
 google_sheet = GoogleSheetDataTable()
 await google_sheet.update_range(service, uri, data, range_address)
```    

 - since service are injected so we need to init it by  

 - similar to create_google_service() in @tests/standard_datable/test_standalone.py
might need to read @tests/standard_datable/README.md
and move create_google_service to a actual useful somewhere

 - then we could call the code by args once after service is initcialized
 - next we need to support GoogleSheetDataTable.update_range by support data as a dataframe(polar not pandas)
 - add test case in standalone test for support dataframe
 - then implement same for append_rows/append_columns/write_new_sheet and test it
 - you might need to change the file i added for some part related PATH and SETTING , while change it as less as possible. so that i could copy back to another repo for integrate test. 
    - i will currently copy this datatable_tools folder until stage6 is complete for usage.
 - since the tool_name in the call_tool_by_sse is looks like 
 ```
 google_sheets__update_range
 ```
 google_sheets is the mcp server name , refer to the service this repo provided.
 so we might need a dict for mapping  tool_name and the Function 
 



#### stage 5.2

 - add the @mcp.tool support data as type : dataframe(polar)
 - note that MCP should not support dataframe yet due to MCP limitation. 
 - so this is just trying to support it , caller from LLM wont pass dataframe to this MCP tool layer, but through this , another component could get to know this mcp tool is support dataframe.
    - another component means planner (that build workflow) will know what tools are avaiable and what input are supported via the MCP protocol list_tools (which return tool & tool desc & input schema & output shcema)
 - test it

### stage6

make the third-party and interface as a package ,so that others could reuse the core : standard datatable api via package install, but able to build their own mcp based on that and keep align with the standard API.

but i wont publish to pip, but need to keep it in private repo . dont might need to later update pyproject to install from git.

### stage7

update the pyproject to install from the package after stage4 fully complete.


now draw out a better plan based on mine
and then later execute after my confirmation 