"""
Setup file for the Alchemy Token Price Fetcher Source connector.
"""

from setuptools import setup, find_packages

setup(
    name="source_alchemy_fetcher",
    description="Source implementation for Alchemy Token Price API.",
    author="Claire",
    author_email="claire@status.im",
    packages=find_packages(),
    install_requires=[
        "airbyte-cdk>=0.51.0",
        "requests>=2.25.0",
    ],
    package_data={
        "source_alchemy_fetcher": ["schemas/*.json", "*.yaml"],
    },
    entry_points={
        "console_scripts": [
            "source-alchemy-fetcher=source_alchemy_fetcher.run:run",
        ],
    },
)
