# Discord Fetcher Source

This is the repository for fetching data from Discord Server, written in Python.

> *Note*: In the rest of the document, the term guild design a discord server (API term)

## Usage

This connector fetches channels and members data from servers instances.

### Configuration

The connector takes the following input:

```yaml
api_key: 'Token  of the bot used'
guilds_id: 
    - 123456789
```

The `guild_id` can be found in  the `server setting` > `Widget` > `Server Id`
The `api-key` is the token of a bot account associated to the servers.


#### Bot Configuration


In order to access the API endpoints of the server, the connector must be authentified as a discord BOT with the priviledge intent `SERVER MEMBERS INTENT`.

For that:
1. Create a Discord Application at https://discord.com/developers/applications
2. Generate a BOT account:
    * Go to the OAuth2 page on the application settings:
        * https://discord.com/developers/applications/<app-id>/oauth2)
    * Store the Client ID and Client Secret in a password manager
    * Select `bot` in the `Oauth2 URL generator` scope and copy the url at the end of the page.
    * Visite the URL and select the Discord Server you want to log into.
3. Configure the BOT token
    * Go to the `Bot` page of the application settings
        - https://discord.com/developers/applications/<app-id>/bot
    * Store the bot token in a  password manager (the `api-key`)
    * Select `SERVER MEMBERS INTENT` in the `Privileged Gateway Intents` category. It will give the bot access to the `members` endpoint.


### Output

The connector will return the following:
- `guild`: List of server information based on the `guilds_id` values.
- `guild_channel`: List of channel for each discord server (contains partial data).
- `channel`: List of channel for each discord server.
- `members`: List of user on the Discord server.

## Local development

### Prerequisites

#### Activate Virtual Environment and install dependencies
From this connector directory, create a virtual environment:
```
python -m venv .venv
```
```
source .venv/bin/activate
pip install -r requirements.txt
```

### Locally running the connector
```
python main.py spec
python main.py check --config sample_files/config-example.json
python main.py discover --config sample_files/config-example.json
python main.py read --config sample_files/config-example.json --catalog sample_files/configured_catalog.json
```

### Locally running the connector docker image

```bash
docker build -t airbyte/twitter-fetcher:dev .
# Running the spec command against your patched connector
docker run airbyte/twitter-fetcher:dev spec
````

#### Run
Then run any of the connector commands as follows:
```
docker run --rm airbyte/twitter-fetcher:dev spec
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/twitter-fetcher:dev check --config /sample_files/config-example.json
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/twitter-fetcher:dev discover --config /sample_files/config-example.json
docker run --rm -v $(pwd)/sample_files:/sample_files -v $(pwd)/sample_files:/sample_files airbyte/twitter-fetcher:dev read --config /sample_files/config-example.json --catalog /sample_files/configured_catalog.json
```
