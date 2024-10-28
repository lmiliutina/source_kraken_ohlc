from setuptools import find_packages, setup

setup(
    entry_points={
        "console_scripts": [
            "source-kraken=source_kraken.run:run",
        ],
    },
    name="source_kraken",
    description="Airbyte Source for Kraken OHLC Data.",
    author="lmilyutina",
    author_email="l.milyutina@thein1.com",
    packages=find_packages(),
    install_requires=["airbyte-cdk", "pytest"],
    package_data={
        "": [
            # Include yaml files in the package (if any)
            "*.yml",
            "*.yaml",
            # Include all json files in the package, up to 4 levels deep
            "*.json",
            "*/*.json",
            "*/*/*.json",
            "*/*/*/*.json",
            "*/*/*/*/*.json",
        ]
    },
)
