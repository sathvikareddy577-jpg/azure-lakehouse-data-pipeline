"""Setup configuration for Azure Lakehouse Data Pipeline"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="azure-lakehouse-pipeline",
    version="1.0.0",
    description="Professional Azure Lakehouse Data Pipeline with Medallion Architecture",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Portfolio Project",
    author_email="",
    url="https://github.com/sathvikareddy577-jpg/azure-lakehouse-data-pipeline",
    license="MIT",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "pyspark>=3.3.0",
        "delta-spark>=2.3.0",
        "python-dotenv>=0.21.0",
        "pydantic>=1.10.0",
        "pyarrow>=10.0.0",
        "pytz>=2022.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0",
            "pytest-cov>=4.0",
            "pytest-mock>=3.10",
            "flake8>=5.0",
            "mypy>=0.99",
            "black>=22.0",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    keywords="azure databricks spark delta-lake medallion-architecture data-engineering",
)
