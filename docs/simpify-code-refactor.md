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
google sheet high level 


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
  remove unused test code  

## important 
most important test case , must not break this test case:
- test_mcp_client_calltool.py 
 this file require start main.py


now draw out a better plan based on mine
and then later execute after my confirmation 