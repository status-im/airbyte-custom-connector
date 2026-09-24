from setuptools import find_packages, setup

setup(
    name="source_elasticsearch_query",
    description="Generic Airbyte source that incrementally searches Elasticsearch.",
    author="Status",
    author_email="devops@status.im",
    packages=find_packages(),
    install_requires=["airbyte-cdk~=0.2", "requests"],
    package_data={"": ["*.json", "*.yaml", "schemas/*.json"]},
    entry_points={
        "console_scripts": [
            "source-elasticsearch-query=source_elasticsearch_query.run:run",
        ],
    },
)
