# ===============================================================================
# Created:        3 Feb 2023
# Updated:
# @author:        Quinlan Eddy
# Description:    Data Factory to store temporary variables
# ===============================================================================


from dataclasses import dataclass

@dataclass
class Auth:
    client_id: str
    device_code: str
    access_token: str
    refresh_token: str = "none"  # Set default to `none`
    token_ttl: int = 2000 # Set default to 2000 seconds (33 minutes)
    