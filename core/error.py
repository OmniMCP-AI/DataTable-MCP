"""
Error classes for DataTable MCP
"""

class UserError(Exception):
    """Base exception for user-related errors"""
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)