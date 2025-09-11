#!/usr/bin/env python3
"""
Simple test script for the Luma API.
This script tests the exact curl request you provided.
"""

import requests
import json
import sys

def test_luma_api(api_key):
    """
    Test the Luma API with the provided API key.

    Args:
        api_key (str): Your Luma API key
    """
    url = "https://public-api.luma.com/v1/calendar/list-events"

    headers = {
        'accept': 'application/json',
        'x-luma-api-key': api_key
    }

    print(f"Testing Luma API...")
    print(f"URL: {url}")
    print(f"Headers: {headers}")
    print("-" * 50)

    try:
        response = requests.get(url, headers=headers, timeout=30)

        print(f"Status Code: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")
        print("-" * 50)

        if response.status_code == 200:
            try:
                data = response.json()
                print("✅ Success! API call returned data:")
                print(json.dumps(data, indent=2))

                # Count events based on Luma API response structure
                if isinstance(data, dict) and 'entries' in data:
                    entries_count = len(data['entries'])
                    print(f"\nFound {entries_count} event entries")

                    # Show pagination info
                    if 'has_more' in data:
                        print(f"Has more pages: {data['has_more']}")
                    if 'next_cursor' in data:
                        print(f"Next cursor: {data['next_cursor']}")

                    # Show sample event data
                    if entries_count > 0:
                        print(f"\nSample event entry:")
                        sample_entry = data['entries'][0]
                        if 'event' in sample_entry:
                            event = sample_entry['event']
                            print(f"  Event ID: {event.get('id', 'N/A')}")
                            print(f"  Event Name: {event.get('name', 'N/A')}")
                            print(f"  Start Time: {event.get('start_at', 'N/A')}")
                            print(f"  Visibility: {event.get('visibility', 'N/A')}")
                        if 'tags' in sample_entry and sample_entry['tags']:
                            tag_names = [tag.get('name', 'N/A') for tag in sample_entry['tags']]
                            print(f"  Tags: {', '.join(tag_names)}")
                elif isinstance(data, list):
                    print(f"\nFound {len(data)} events")
                elif isinstance(data, dict) and 'events' in data:
                    print(f"\nFound {len(data['events'])} events")
                elif isinstance(data, dict) and 'data' in data:
                    print(f"\nFound {len(data['data'])} events")
                else:
                    print("\nSingle event or different structure returned")

            except json.JSONDecodeError:
                print("❌ Response is not valid JSON:")
                print(response.text)
        else:
            print(f"❌ API call failed with status {response.status_code}")
            print(f"Response: {response.text}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python test_luma_api.py <your_api_key>")
        print("Example: python test_luma_api.py luma_api_key_xxx")
        sys.exit(1)

    api_key = sys.argv[1]
    test_luma_api(api_key)
