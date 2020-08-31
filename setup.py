from setuptools import setup, find_packages


setup(
    name="servicelayer",
    version="1.14.0",
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
    url="http://github.com/alephdata/servicelayer",
    license="MIT",
    packages=find_packages(exclude=["ez_setup", "examples", "test"]),
    namespace_packages=[],
    include_package_data=True,
    zip_safe=True,
    install_requires=[
        "banal >= 1.0.1",
        "normality >= 2.1.1",
        "redis == 3.5.3",
        "fakeredis == 1.4.3",
        "sqlalchemy >= 1.3",
    ],
    extras_require={
        "amazon": ["boto3 >= 1.11.9"],
        "google": ["grpcio == 1.31.0", "google-cloud-storage == 1.31.0"],
        "dev": [
            "twine",
            "moto",
            "boto3 >= 1.11.9",
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
