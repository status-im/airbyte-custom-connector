import logging
import json


def extract_token(token_data):
    name= 'No Name' if 'name' not in token_data['tokenInfo'] else token_data['tokenInfo']['name']
    description= 'No description available' if 'description' not in token_data['tokenInfo'] else token_data['tokenInfo']['description']
    symbol= 'No Symbol' if 'symbol' not in token_data['tokenInfo'] else token_data['tokenInfo']['symbol']
    try:
        token = {
            "name": name,
            "symbol": symbol,
            "description": description,
            "address":token_data['tokenInfo']['address'],
            "chain": "Ethereum",
            "balance": token_data['rawBalance'],
            "decimal": token_data['tokenInfo']['decimals']
        }
        return token
    except KeyError:
        logging.error("Error when trying to extract data from token %s"  % tokens_data)
        return None


