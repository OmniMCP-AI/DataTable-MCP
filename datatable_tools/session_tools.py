from typing import Dict, List, Optional, Any
import logging
import asyncio
from datetime import datetime, timedelta
from core.server import register_tool
from datatable_tools.table_manager import table_manager

logger = logging.getLogger(__name__)

async def cleanup_tables(
    force_cleanup: bool = False,
    table_ids: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Clean up expired tables or specific tables from memory.

    Args:
        force_cleanup: If True, removes all tables regardless of TTL
        table_ids: Optional list of specific table IDs to remove

    Returns:
        Dict containing cleanup operation results
    """
    try:
        if table_ids:
            # Clean up specific tables
            cleaned_count = 0
            not_found = []

            for table_id in table_ids:
                if table_manager.delete_table(table_id):
                    cleaned_count += 1
                else:
                    not_found.append(table_id)

            return {
                "success": True,
                "cleanup_type": "specific_tables",
                "cleaned_count": cleaned_count,
                "requested_count": len(table_ids),
                "not_found": not_found,
                "message": f"Cleaned up {cleaned_count} out of {len(table_ids)} requested tables"
            }
        else:
            # Clean up expired tables (or all if force_cleanup=True)
            cleaned_count = table_manager.cleanup_expired_tables(force=force_cleanup)

            return {
                "success": True,
                "cleanup_type": "expired_tables" if not force_cleanup else "all_tables",
                "cleaned_count": cleaned_count,
                "force_cleanup": force_cleanup,
                "message": f"Cleaned up {cleaned_count} {'expired' if not force_cleanup else ''} tables"
            }

    except Exception as e:
        logger.error(f"Error during table cleanup: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": "Failed to clean up tables"
        }

async def get_table_info(
    table_id: str,
    include_sample: bool = False,
    sample_rows: int = 5
) -> Dict[str, Any]:
    """
    Get detailed information about a specific table.

    Args:
        table_id: ID of the table to inspect
        include_sample: If True, includes sample data
        sample_rows: Number of sample rows to include

    Returns:
        Dict containing detailed table information
    """
    try:
        table = table_manager.get_table(table_id)
        if not table:
            return {
                "success": False,
                "error": f"Table {table_id} not found",
                "message": "Table does not exist"
            }

        # Basic table information
        info = {
            "success": True,
            "table_id": table_id,
            "name": table.metadata.name,
            "shape": table.shape,
            "headers": table.headers,
            "dtypes": table.dtypes,
            "created_at": table.metadata.created_at,
            "last_modified": table.metadata.last_modified,
            "source_info": table.metadata.source_info,
            "ttl_minutes": table.metadata.ttl_minutes
        }

        # Add memory usage info
        info["memory_usage"] = {
            "rows": len(table.df),
            "columns": len(table.df.columns),
            "memory_mb": round(table.df.memory_usage(deep=True).sum() / 1024 / 1024, 2)
        }

        # Add data quality info
        info["data_quality"] = {
            "null_counts": table.df.isnull().sum().to_dict(),
            "duplicate_rows": int(table.df.duplicated().sum()),
            "unique_counts": table.df.nunique().to_dict()
        }

        # Add sample data if requested
        if include_sample and len(table.df) > 0:
            sample_size = min(sample_rows, len(table.df))
            info["sample_data"] = table.df.head(sample_size).to_dict('records')
            info["sample_size"] = sample_size

        return info

    except Exception as e:
        logger.error(f"Error getting table info for {table_id}: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to get information for table {table_id}"
        }

async def get_session_stats() -> Dict[str, Any]:
    """
    Get statistics about the current session and all active tables.

    Returns:
        Dict containing session statistics
    """
    logger.info("get_session_stats called")
    try:
        tables_info = table_manager.list_tables()
        current_time = datetime.now()

        # Calculate statistics
        total_tables = len(tables_info)
        total_memory_mb = 0
        total_rows = 0
        total_columns = 0

        expired_tables = []
        table_ages = []

        for table_info in tables_info:
            table = table_manager.get_table(table_info["table_id"])
            if table:
                # Memory usage
                memory_mb = table.df.memory_usage(deep=True).sum() / 1024 / 1024
                total_memory_mb += memory_mb

                # Data counts
                total_rows += table.shape[0]
                total_columns += table.shape[1]

                # Age calculation
                created_time = datetime.fromisoformat(table.metadata.created_at)
                age_minutes = (current_time - created_time).total_seconds() / 60
                table_ages.append(age_minutes)

                # Check if expired
                ttl_minutes = table.metadata.ttl_minutes
                if age_minutes > ttl_minutes:
                    expired_tables.append({
                        "table_id": table.table_id,
                        "name": table.metadata.name,
                        "age_minutes": round(age_minutes, 2),
                        "ttl_minutes": ttl_minutes
                    })

        return {
            "success": True,
            "session_stats": {
                "total_tables": total_tables,
                "total_memory_mb": round(total_memory_mb, 2),
                "total_rows": total_rows,
                "total_columns": total_columns,
                "average_table_age_minutes": round(sum(table_ages) / len(table_ages), 2) if table_ages else 0,
                "expired_tables_count": len(expired_tables),
                "expired_tables": expired_tables
            },
            "cleanup_recommendations": {
                "should_cleanup": len(expired_tables) > 0,
                "expired_count": len(expired_tables),
                "memory_savings_mb": sum(
                    table_manager.get_table(exp["table_id"]).df.memory_usage(deep=True).sum() / 1024 / 1024
                    for exp in expired_tables
                    if table_manager.get_table(exp["table_id"])
                ) if expired_tables else 0
            },
            "message": f"Session has {total_tables} active tables using {round(total_memory_mb, 2)} MB memory"
        }

    except Exception as e:
        logger.error(f"Error getting session stats: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": "Failed to get session statistics"
        }

# Periodic cleanup task (would be called by a scheduler in production)
async def periodic_cleanup():
    """Background task to periodically clean up expired tables"""
    while True:
        try:
            await asyncio.sleep(300)  # Run every 5 minutes
            cleaned = table_manager.cleanup_expired_tables()
            if cleaned > 0:
                logger.info(f"Periodic cleanup: removed {cleaned} expired tables")
        except Exception as e:
            logger.error(f"Error in periodic cleanup: {e}")
            await asyncio.sleep(60)  # Wait 1 minute before retrying

# Register all tool functions
register_tool("cleanup_expired_tables", cleanup_tables)
register_tool("get_table_info", get_table_info) 
register_tool("get_session_info", get_session_stats)