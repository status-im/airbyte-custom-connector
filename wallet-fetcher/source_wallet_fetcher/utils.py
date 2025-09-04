# Number used as decimal when the contract has been destroyed
INVALID_DECIMAL_NUMBER = "22270923681254677845691103109158760375340177724800803888364822332811285364736"

def check_token_validity(token_info):
    """Check if token contract is still valid"""
    if "decimals" not in token_info:
        raise Exception('Invalid token: decimal fields not present')
    
    if str(token_info.get("decimals")) == INVALID_DECIMAL_NUMBER:
        raise Exception('Invalid token: decimal fields invalid')

def extract_token(wallet_name, token_data):
    """Extract token information and convert to string format"""
    try:
        if not isinstance(token_data, dict) or 'tokenInfo' not in token_data:
            raise Exception('Invalid token data structure')
            
        token_info = token_data['tokenInfo']
        
        # Get basic token info with defaults
        name = token_info.get('name', 'Unknown Token')
        symbol = token_info.get('symbol', 'No Symbol')
        description = token_info.get('description', 'No description available')
        
        # Validate token
        check_token_validity(token_info)
        
        # Convert balance and decimals to strings
        raw_decimals = str(token_info.get('decimals', '18'))
        raw_balance = str(token_data.get('balance', '0'))
        
        # Skip tokens with invalid decimals
        if raw_decimals == INVALID_DECIMAL_NUMBER:
            raise Exception(f"Invalid decimal value for token {name}")
        
        # Calculate human-readable balance
        try:
            decimals = int(raw_decimals)
            balance_val = float(raw_balance)
            human_balance = balance_val / (10 ** decimals)
            balance_str = str(human_balance)
        except (ValueError, OverflowError, ZeroDivisionError):
            balance_str = "0"
        
        return {
            "wallet_name": wallet_name,
            "name": name,
            "symbol": symbol,
            "description": description,
            "address": token_info.get('address', ''),
            "chain": "Ethereum",
            "balance": balance_str,
            "decimal": raw_decimals,
        }
        
    except Exception as err:
        raise Exception(f'Error processing token: {err}')