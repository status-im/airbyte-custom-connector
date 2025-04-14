"""
Setup file for the Google Play Source connector.
"""

from setuptools import setup, find_packages

setup(
    name="source_app_store",
    description="Source implementation for Apple Store Data.",
    author="Claire",
    author_email="claire@status.im",
    packages=find_packages(),
    install_requires=[
        "airbyte-cdk~=0.2",
        "google-api-python-client>=2.0.0",
    
    ],
    #package_data={"": ["*.json", "*.yaml", "schemas/*.json", "schemas/shared/*.json"]},
    package_data={
        "source_app_store": ["schemas/*.json", "*.yaml"],
    },
    entry_points={
        "console_scripts": [
            "source-app-store=source_app_store.run:run",
        ],
    },
) 

