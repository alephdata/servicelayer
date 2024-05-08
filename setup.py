from setuptools import setup, find_packages

with open("README.md") as f:
    long_description = f.read()


setup(
    name="servicelayer",
    version="1.23.0-rc6",
    description="Basic remote service functions for alephdata components",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    keywords="storage files s3",
    author="Organized Crime and Corruption Reporting Project",
    author_email="data@occrp.org",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="http://github.com/alephdata/servicelayer",
    license="MIT",
    packages=find_packages(exclude=["ez_setup", "examples", "test"]),
    namespace_packages=[],
    include_package_data=True,
    zip_safe=True,
    install_requires=[
        "banal >= 1.0.6, < 2.0.0",
        "normality >= 2.4.0, < 3.0.0",
        "fakeredis >= 2.11.2, < 3.0.0",
        "redis <= 4.6.0",
        "sqlalchemy >= 1.4.49, < 3.0.0",
        "structlog >= 20.2.0, < 25.0.0",
        "colorama >= 0.4.4, < 1.0.0",
        "pika >= 1.3.1, < 2.0.0",
        "prometheus-client >= 0.17.1, < 0.21.0",
    ],
    extras_require={
        "amazon": ["boto3 >= 1.11.9, <2.0.0"],
        "google": [
            "grpcio >= 1.32.0, <2.0.0",
            "google-cloud-storage >= 1.31.0, < 3.0.0",
        ],
        "dev": [
            "twine",
            "moto < 5",
            "boto3 >= 1.11.9, <2.0.0",
            "pytest >= 3.6",
            "coverage",
            "pytest-cov",
        ],
    },
    test_suite="tests",
    entry_points={
        "servicelayer.test": ["test = servicelayer.extensions:get_extensions"]
    },
)
