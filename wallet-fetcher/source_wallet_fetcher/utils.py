import logging
import json


def extract_token(wallet_name, token_data):
    description= 'No description available' if 'description' not in token_data['tokenInfo'] else token_data['tokenInfo']['description']
    symbol= 'No Symbol' if 'symbol' not in token_data['tokenInfo'] else token_data['tokenInfo']['symbol']
    try:
        if token_data['tokenInfo']['decimals'] == '22270923681254677845691103109158760375340177724800803888364822332811285364736':
            # The data is not valid, droping it.
            raise Exception('Invalid token: decimal fields not valid: %s', token_data)
        if token_data['tokenInfo']['owner'] == '0x0000000000000000000000000000000000000000':
            raise Exception('Invalid token, owner is the void address: %s', token_data)
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
