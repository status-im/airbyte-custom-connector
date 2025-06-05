"""
Setup file for the Youtube Source connector.
"""

from setuptools import find_packages, setup

MAIN_REQUIREMENTS = [
    "airbyte-cdk==0.1.104",
    "google-api-python-client",
    "requests",
    "python-dotenv",
]

TEST_REQUIREMENTS = [
    "requests-mock==1.9.3",
    "pytest~=6.2",
    "pytest-mock~=3.6.1",
    "connector-acceptance-test",
]

setup(
    name="source_youtube_fetcher",
    description="Source implementation for YouTube Data v3 API.",
    author="Airbyte",
    author_email="contact@airbyte.io",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    package_data={"": ["*.json", "*.yaml", "schemas/*.json", "schemas/*/*.json"]},
    extras_require={
        "tests": TEST_REQUIREMENTS,
    },
    entry_points={
        "console_scripts": [
            "source-youtube-fetcher=source_youtube_fetcher.run:run",
        ],
    },
) 

