import logging
import json

# Number used as decimal when the contract has been destroyed
INVALID_DECIMAL_NUMBER="22270923681254677845691103109158760375340177724800803888364822332811285364736"
VOID_ADDRESS="0x0000000000000000000000000000000000000000"

"""
Function to verify if the tokens  contracts is still valid
"""
def check_token_validity(tokenInfo):
    if "decimals" not in tokenInfo:
        raise Exception('Invalid token: decimal fields not present: %s', tokenInfo)
    if tokenInfo.get("decimals") == INVALID_DECIMAL_NUMBER:
        raise Exception('Invalid token: decimal fields invalid: %s', tokenInfo)

def extract_token(wallet_name, token_data):
    description= 'No description available' if 'description' not in token_data['tokenInfo'] else token_data['tokenInfo']['description']
    symbol= 'No Symbol' if 'symbol' not in token_data['tokenInfo'] else token_data['tokenInfo']['symbol']
    try:
        check_token_validity(token_data['tokenInfo'])
        token = {
            "wallet_name": wallet_name,
            "name": token_data['tokenInfo']['name'],
            "symbol": symbol,
            "description": description,
            "address":token_data['tokenInfo']['address'],
            "chain": "Ethereum",
            "balance": token_data['balance'],
            "decimal": token_data['tokenInfo']['decimals']
        }
        return token
    except KeyError as err:
        raise Exception('Invalid ERC-20 Token: %s', err)
