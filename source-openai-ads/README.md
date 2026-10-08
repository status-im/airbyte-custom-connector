# OpenAI Ads

Pulls one OpenAI ad account. The key is sent as `Authorization: Bearer`. Reporting reference: https://developers.openai.com/ads/reporting

## Configuration

`api_key` is the Ads API key for one ad account. Each sync requests the last 7 days in the account timezone.

## Streams

`ad_account` is the account name, timezone, currency, and review status.

`campaigns` is every campaign. `budget.lifetime_spend_limit_micros` is the cap in millionths of the currency unit (`25000000` is 25.00). That is the budget, not the amount spent.

`ad_groups` and `ads` are the child objects, with `campaign_id` (and `ad_group_id` on ads) added so they join without another lookup.

`event_settings` is the conversion definition: event type, attribution window, and linked campaigns.

`campaign_insights` is one row per campaign per day from `GET /ad_account/insights`. This is the spend and conversion table. `spend` is in major units (`12.50`), not micros. `conversions` counts goal conversions from clicks and views together. The same row has impressions, clicks, ctr, cpc, cpm, cpa, post_click_cvr, attributed purchase value, and ROAS. Join `campaigns` on `campaign_id` for status and budget.

`campaign_conversions` is one row per campaign per day from `POST /conversions/insights`. `click_through_conversions` and `view_through_conversions` split the goal total. `attributed_events` lists each event name, including events that are not the campaign goal.

`campaign_insights_country`, `campaign_insights_device`, `campaign_insights_platform`, and `campaign_insights_product` split delivery and spend for each campaign day. The API accepts one breakdown per request, so each breakdown is its own stream. `campaign_conversions_country` and `campaign_conversions_device` split goal conversions the same way, including click-through and view-through. Platform and product breakdowns carry delivery and spend. Country and device breakdowns also carry conversions on the conversion streams. A breakdown row is present where that slice had delivery, so adding the rows up can differ from `campaign_insights`.

Both daily streams use the API default attribution: 30-day click, 1-day view, and ad-event time. Those three values are columns on every row. A null metric means the API could not calculate it. It does not mean zero.

Primary keys are `campaign_id` + `readable_time` and `campaign_id` + `date`. Sync full refresh. Append dedup keeps days already stored. Overwrite leaves only the 7 days from the latest sync.

## Local development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py spec
python main.py check --config sample_files/config-example.json
python main.py read --config sample_files/config-example.json --catalog sample_files/configured_catalog.json
```
