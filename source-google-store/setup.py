"""
Setup file for the Google Play Source connector.
"""

from setuptools import setup, find_packages

setup(
    name="source_google_store",
    description="Source implementation for Google Play Store Data.",
    author="Claire",
    author_email="claire@status.im",
    packages=find_packages(),
    install_requires=[
        "airbyte-cdk~=0.2",
        "google-api-python-client>=2.0.0",
        "oauth2client>=4.1.3",
        "httplib2>=0.20.0",
    ],
    package_data={"": ["*.json", "*.yaml", "schemas/*.json", "schemas/shared/*.json"]},
    entry_points={
        "console_scripts": [
            "source-discord-fetcher=source_discord_fetcher.run:run",
        ],
    },
) 

