"""Package metadata for the Azure Lakehouse portfolio project."""

from setuptools import find_packages, setup

with open("README.md", encoding="utf-8") as readme:
    long_description = readme.read()


setup(
    name="azure-lakehouse-pipeline",
    version="2.0.0",
    description="Tested medallion lakehouse pipeline with PySpark, Delta Lake, and Azure",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/sathvikareddy577-jpg/azure-lakehouse-data-pipeline",
    license="MIT",
    packages=find_packages(),
    python_requires=">=3.10,<3.13",
    install_requires=[
        "pyspark==3.5.5",
        "delta-spark==3.3.2",
        "python-dotenv==1.1.1",
    ],
    extras_require={
        "dev": [
            "pytest==8.4.2",
            "pytest-cov==6.3.0",
            "ruff==0.12.12",
        ]
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
)
