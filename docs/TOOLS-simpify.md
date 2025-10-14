# DataTable Tools Reference v2

This document provides an improved, LLM-friendly reference for all available tools in the DataTable server. This version follows best practices for tool naming, parameter design, and schema clarity.
This tools spec will be use to build a in-memory table as a standard, and Google Sheet MCP tool, Excel MCP tool will use it as guileline for development.

## Design Principles

1. **Explicit Function Names**: Action-oriented verb + object pattern (e.g., `create_table`, `add_row`)
2. **No Parameter Overloading**: Separate functions instead of optional parameters where it improves clarity
3. **Descriptive Types**: Avoid `Any` types; use specific types like `List`, `Dict[str, Any]`, etc.
4. **Minimal Optionals**: Provide separate functions rather than many optional parameters
5. **LLM-Friendly**: Clear intent matching, easy parameter inference, intuitive descriptions

## Overview

The DataTable MCP server provides **16 tools** organized into **5 categories**:
- **Table Lifecycle Management** (4 tools)
- **Data Manipulation** (7 tools)
- **Data Query & Access** (2 tools)
- **Export & Persistence** (1 tool)
- **Advanced Operations** (3 tools)

## Common Response Format

All tools return responses in this standardized format:

```json
{
  "success": true/false,
  "error": "error message if success=false",
  "message": "human-readable status message",
  // ... tool-specific data
}
```

---
write_new_sheet
append_rows
append_columns
update_range
