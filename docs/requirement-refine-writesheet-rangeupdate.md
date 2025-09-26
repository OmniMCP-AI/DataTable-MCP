 #requirement 
now consider for details operation like update cell / row /range ,
  do you had a better plan , would it use the current endpoint
  /range/update or /worksheet/write_sheet would be better. are other factors i need to consider .
- consider
  the pamater requires to pass into the endpoint ,and i'd preferred it
  would better to be simple. 
- need more robust 
 
1) read the code another repo  in /Users/dengwei/work/ai/om3/api4
endpoint '/Users/dengwei/work/ai/om3/api4/src/fastestai/tools/sheet/worksheet/api.py'
implement /Users/dengwei/work/ai/om3/api4/src/fastestai/tools/sheet/worksheet/service.py

access /api/v1/tool/sheet/worksheet/read_sheet and /api/v1/tool/sheet/worksheet/write_sheet 

2) you might need to consider /update for cell/row/column operation, but not needed in this requirement implement
'/Users/dengwei/work/ai/om3/api4/src/fastestai/tools/sheet/range/api.py'
'/Users
/dengwei/work/ai/om3/api4/src/fastestai/tools/sheet/range/service.py'
