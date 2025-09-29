from typing import Dict, List, Optional, Any
import logging
import io
import os
import pandas as pd
from pathlib import Path
from fastmcp import Context
from core.server import mcp
from datatable_tools.table_manager import table_manager

logger = logging.getLogger(__name__)

# Note: export_table functionality has been merged into export_table_to_range in detailed_tools.py
# Note: export_data_to_uri functionality has been merged into update_range in detailed_tools.py