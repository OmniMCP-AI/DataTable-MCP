# requirement

now in order implement /datatable/load from spreadsheet and /datatable/export to spreadsheet
i need u to :
1) access another api endpoint with env speic the endpoint SPREADSHEET_API env (http://localhost:9394) to access /api/v1/tool/sheet/worksheet/read_sheet and /api/v1/tool/sheet/worksheet/write_sheet 
2) read the code another repo  in /Users/dengwei/work/ai/om3/api4
endpoint '/Users/dengwei/work/ai/om3/api4/src/fastestai/tools/sheet/worksheet/api.py'
implement /Users/dengwei/work/ai/om3/api4/src/fastestai/tools/sheet/worksheet/service.py
3) refactor the code in this repo 
/datatable/load and /datatable/export 
- make nessiary change , and has different class implement accordingly (spreadsheet/excel/database)
- add nessisary request model for validation
- note that it require user-id 
4) test it and verify the intergration 
- for test you could use user-id
 TEST_USER_ID = "68501372a3569b6897673a48"  # Wade's user ID from existing tests
- reference test code 
'/Users/dengwei/work/ai/om3/api4/tests/tools/sheet/test_write_sheet_creation.py'
'/Users/dengwei/work/ai/om3/api4/tests/tools/sheet/test_update_range.py'


this use  /v1/tool/sheet//range/update endpoint 

5) the docker should has an env for SPREADSHEET_API endpoint