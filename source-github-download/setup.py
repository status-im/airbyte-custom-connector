from setuptools import find_packages, setup

MAIN_REQUIREMENTS = [
    "airbyte-cdk~=0.2",
]

TEST_REQUIREMENTS = [
    "requests-mock~=1.9.3",
    "pytest~=6.2",
    "pytest-mock~=3.6.1",
    "connector-acceptance-test",
]

setup(
    name="source_github_download",
    description="Source implementation for GitHub Release Download Counts.",
    author="Airbyte",
    author_email="claire@status.im",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    package_data={"": ["*.json", "*.yaml", "schemas/*.json"]},
    extras_require={
        "tests": TEST_REQUIREMENTS,
    },
    entry_points={
        "console_scripts": [
            "source-github-download=source_github_download.run:run",
        ],
    },
)
