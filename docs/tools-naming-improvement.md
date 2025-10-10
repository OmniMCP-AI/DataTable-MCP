# improvement tool naming and params design 
## backgound previouly TOOLS.md has summary the too provide to used by other developer, so that this could be an standard API for implement database table like table so in the future , will other developer use the standard to implement the excel version , google sheet verion , and self define sheet version, rather than each has its own standard. these standard is use internally.
 
 but the expose MCP tool API with @mcp.tool will had other standard which need to be friendly to LLM


## requirement
 - right now i wanna improve the naming and params
    name would preferred  verb+obj or   verb+obj+by+obj as standard
 - merge similar tools 
    such as search_table and list_table should be very similar  

## rules
 - to be use more explicitly, try not overload function parameters
 - need to be easily understand 
 - try not use many Optional , if needed , give a new function
 - try not use Any as data input type, for example more descriptive 
 ```
    async def update_cell(
    ctx: Context,
    table_id: str,
    row_index: int,
    column: Union[str, int],
    value: Any
) -> Dict[str, Any]: 
```

 preferred to be 

```
 async def update_cell_by_row_index(
    ctx: Context,
    table_id: str,
    row_index: List[int],
    value: List
) -> Dict[str, Any]:
```

example usage for input 
```
{
 "table_id": "table_abc123",           // Required
 "row_indices": [0, 1],               // Required: list of row indices
 "values": [[26, "Senior Engineer"], [31, "Lead Designer"]],  // Required: 2D array
 "fill_strategy": "none"              // Optional: "none", "fill_na", "fill_empty", "fill_zero"
}
```

## step 
- improve the TOOLS.md docs first  , DONT MODIFY THE CODE YET
- focus on the most common use tool first
- if things unclear , raise a question to confirm

## reference you could use for guideline
@reference/best-practise-tool.md
@reference/llm-chosse-the-right-tool.md

## further more
 