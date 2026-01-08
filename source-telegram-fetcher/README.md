# Telegram Fetcher Source

This is an Airbyte source connector for fetching messages from Telegram using the Bot API.

## Overview

This connector uses [Telegram Bot API](https://core.telegram.org/bots/api) to fetch messages from chats where your bot is a member.

### Important: Message Retention

⚠️ **Telegram only retains unacknowledged bot updates for approximately 24 hours.**

This means:
- Once messages are fetched, they are acknowledged and removed from Telegram's queue
- If the connector doesn't run for >24 hours, older messages will be lost
- **Recommendation**: Run the connector every 6 hours to ensure no messages are missed

## Streams

| Stream | Description | Sync Mode | Destination Mode |
|--------|-------------|-----------|------------------|
| `messages` | Messages from all chats the bot has access to | Full Refresh | **Append** (each run adds new messages only) |
| `chat_info` | Information about configured chats including member count | Full Refresh | Overwrite |

> **Note**: The `messages` stream always returns only NEW, unacknowledged messages. Telegram handles deduplication server-side — once fetched, messages are removed from the queue.

## Configuration

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `bot_token` | string | Yes | Bot token from @BotFather |
| `chat_ids` | array | No | List of chat IDs to filter (leave empty for all chats) |

### Getting Your Bot Token

1. Open Telegram and search for @BotFather
2. Send `/newbot` and follow the prompts
3. Copy the bot token provided

### Getting Chat IDs

Chat IDs for groups/supergroups are negative numbers (e.g., `-1001234567890`).

To find a chat ID:
1. Add your bot to the group
2. Send a message in the group
3. Run the connector once or use `https://api.telegram.org/bot<TOKEN>/getUpdates`
4. Look for the `chat.id` field in the response

## Local Development

### Setup

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -e .
```

### Running Locally

```bash
# Check connection
python main.py check --config sample_files/config.json

# Discover streams
python main.py discover --config sample_files/config.json

# Read data
python main.py read --config sample_files/config.json --catalog sample_files/configured_catalog.json
```

### Building Docker Image

```bash
docker build -t source-telegram-fetcher:dev .

# Run with Docker
docker run --rm source-telegram-fetcher:dev check --config '{"bot_token": "YOUR_TOKEN"}'
```
