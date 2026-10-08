from setuptools import find_packages, setup

MAIN_REQUIREMENTS = [
    "airbyte-cdk>=0.50.0,<0.60.0",
    "requests>=2.31.0",
]

setup(
    name="source_openai_ads",
    description="Source implementation for the OpenAI Ads API.",
    author="Status",
    author_email="devops@status.im",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    package_data={"": ["*.json", "*.yaml", "schemas/*.json"]},
    entry_points={
        "console_scripts": [
            "source-openai-ads=source_openai_ads.run:run",
        ],
    },
)
