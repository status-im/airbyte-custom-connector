from setuptools import find_packages, setup

MAIN_REQUIREMENTS = [
    "airbyte-cdk~=0.2",
    "requests",
]

setup(
    name="source_github_search_count",
    description="Airbyte source that tracks GitHub code search result counts.",
    author="Airbyte",
    author_email="claire@status.im",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    package_data={"": ["*.json", "*.yaml", "schemas/*.json"]},
    entry_points={
        "console_scripts": [
            "source-github-search-count=source_github_search_count.run:run",
        ],
    },
)
