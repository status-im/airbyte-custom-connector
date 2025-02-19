# SimpleCast Fetcher Source

This is the repository for fetching SimpleCast data, written in Python.

## Usage

The connector fetch the list of podcasts, episodes and analytics metrics from [SimpleCast](https://www.simplecast.com/).

For more information on the endpoints available: https://apidocs.simplecast.com/


### Configuration

The connector takes the following input:

```yaml
- api_key
```

> SimpleCast provide a public test api key: `eyJhcGlfa2V5IjoiYWIwMDI3NDMzZDUyMzNmYWFhMTcwZjI4ZDBjNjY2ODIifQ==` 


### Output

The connector will return the following objects:
- [podcast](./source_simplecast_fecther/schemas/podcast.json)
- [episode](./source_simplecast_fecther/schemas/episode.json)
- [EpisodeDownload](./source_simplecast_fecther/schemas/episode_download.json)
- [PodcastListeningLocation](./source_simplecast_fecther/schemas/podcast_listening_location.json)
- [PodcastListeningDevice](./source_simplecast_fecther/schemas/podcast_listening_device.json)
- [PodcastListeningMethod](./source_simplecast_fecther/schemas/podcast_listening_method.json)


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
