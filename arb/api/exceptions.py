"""
Custom exceptions related to Deribit API interactions.
"""

class DeribitAPIError(Exception):
    """Custom exception for errors returned by the Deribit API."""
    def __init__(self, error_data: dict):
        self.code = error_data.get('code')
        self.message = error_data.get('message', 'Unknown API error')
        self.data = error_data.get('data')
        super().__init__(f"Deribit API Error {self.code}: {self.message} {self.data or ''}")

    def __str__(self):
        return f"DeribitAPIError(code={self.code}, message='{self.message}', data={self.data})"

# Add other potential custom exceptions if needed, e.g., AuthenticationError
# class AuthenticationError(DeribitAPIError):
#     pass