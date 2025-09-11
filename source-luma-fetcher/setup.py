"""
Setup file for the Google Play Source connector.
"""

from setuptools import setup, find_packages

setup(
    name="source_luma_fetcher",
    description="Source implementation for Luma Data.",
    author="Claire",
    author_email="claire@status.im",
    packages=find_packages(),
    install_requires=[
        "airbyte-cdk~=0.2",
        "google-api-python-client>=2.0.0",
        "oauth2client>=4.1.3",
        "httplib2>=0.20.0",
    ],
    #package_data={"": ["*.json", "*.yaml", "schemas/*.json", "schemas/shared/*.json"]},
    package_data={
        "source_luma_fetcher": ["schemas/*.json", "*.yaml"],
    },
    entry_points={
        "console_scripts": [
            "source-luma-fetcher=source_luma_fetcher.run:run",
        ],
    },
)
