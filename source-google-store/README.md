# Google Play Store Reviews Source

This is the repository for the Google Play Store Reviews source connector, written in Python.
For information about how to use this connector within Airbyte, see [the documentation](https://docs.airbyte.com/integrations/sources/google-store).

## Overview

This connector extracts reviews data from the Google Play Store for your Android application. It uses the Google Play Store data export functionality via Google Cloud Storage to access the reviews data.

### Output schema

This connector outputs the following stream:

1. **Reviews** - User reviews for your Android app

### Features

| Feature | Supported? |
| --- | --- |
| Full Refresh Sync | Yes |
| Incremental Sync | No |
| Namespaces | No |

## Local development

### Prerequisites

* Python (`^3.10`)
* Poetry (`^1.7`) - installation instructions [here](https://python-poetry.org/docs/#installation)



### Installing the connector

From this connector directory, run:
```bash
poetry install --with dev
```


### Create credentials

**If you are a community contributor**, follow the instructions in the [documentation](https://docs.airbyte.com/integrations/sources/google-store)
to generate the necessary credentials. Then create a file `secrets/config.json` conforming to the `src/source_google_store/spec.yaml` file.
Note that any directory named `secrets` is gitignored across the entire Airbyte repo, so there is no danger of accidentally checking in sensitive information.
See `sample_files/sample_config.json` for a sample config file.


### Locally running the connector

```
poetry run source-google-store spec
poetry run source-google-store check --config secrets/config.json
poetry run source-google-store discover --config secrets/config.json
poetry run source-google-store read --config secrets/config.json --catalog sample_files/configured_catalog.json
```

### Running tests

To run tests locally, from the connector directory run:

```
poetry run pytest tests
```

### Building the docker image

1. Install [`airbyte-ci`](https://github.com/airbytehq/airbyte/blob/master/airbyte-ci/connectors/pipelines/README.md)
2. Run the following command to build the docker image:
```bash
airbyte-ci connectors --name=source-google-store build
```

An image will be available on your host with the tag `airbyte/source-google-store:dev`.


### Running as a docker container

Then run any of the connector commands as follows:
```
docker run --rm airbyte/source-google-store:dev spec
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-google-store:dev check --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-google-store:dev discover --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets -v $(pwd)/integration_tests:/integration_tests airbyte/source-google-store:dev read --config /secrets/config.json --catalog /integration_tests/configured_catalog.json
```

### Running our CI test suite

You can run our full test suite locally using [`airbyte-ci`](https://github.com/airbytehq/airbyte/blob/master/airbyte-ci/connectors/pipelines/README.md):

```bash
airbyte-ci connectors --name=source-google-store test
```

### Customizing acceptance Tests

Customize `acceptance-test-config.yml` file to configure acceptance tests. See [Connector Acceptance Tests](https://docs.airbyte.com/connector-development/testing-connectors/connector-acceptance-tests-reference) for more information.
If your connector requires to create or destroy resources for use during acceptance tests create fixtures for it and place them inside integration_tests/acceptance.py.

### Dependency Management

All of your dependencies should be managed via Poetry. 
To add a new dependency, run:

```bash
poetry add <package-name>
```

Please commit the changes to `pyproject.toml` and `poetry.lock` files.

## Publishing a new version of the connector

You've checked out the repo, implemented a million dollar feature, and you're ready to share your changes with the world. Now what?
1. Make sure your changes are passing our test suite: `airbyte-ci connectors --name=source-google-store test`
2. Bump the connector version (please follow [semantic versioning for connectors](https://docs.airbyte.com/contributing-to-airbyte/resources/pull-requests-handbook/#semantic-versioning-for-connectors)): 
    - bump the `dockerImageTag` value in in `metadata.yaml`
    - bump the `version` value in `pyproject.toml`
3. Make sure the `metadata.yaml` content is up to date.
4. Make sure the connector documentation and its changelog is up to date (`docs/integrations/sources/google-store.md`).
5. Create a Pull Request: use [our PR naming conventions](https://docs.airbyte.com/contributing-to-airbyte/resources/pull-requests-handbook/#pull-request-title-convention).
6. Pat yourself on the back for being an awesome contributor.
7. Someone from Airbyte will take a look at your PR and iterate with you to merge it into master.
8. Once your PR is merged, the new version of the connector will be automatically published to Docker Hub and our connector registry.



python3.11 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

 python3.11 main.py spec
python3.11 main.py check --config sample_files/config-example.json

docker buildx build -t source-google-store:dev .
 docker run source-google-store:dev spec