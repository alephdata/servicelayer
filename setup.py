from setuptools import setup, find_packages


setup(
    name='servicelayer',
    version='1.11.1',
    description="Basic remote service functions for alephdata components",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7'
    ],
    keywords='storage files s3',
    author='Organized Crime and Corruption Reporting Project',
    author_email='data@occrp.org',
    url='http://github.com/alephdata/servicelayer',
    license='MIT',
    packages=find_packages(exclude=['ez_setup', 'examples', 'test']),
    namespace_packages=[],
    include_package_data=True,
    zip_safe=True,
    install_requires=[
        'banal >= 0.4.2',
        'six >= 1.12.0',
        'normality >= 1.0.0',
        'redis == 3.5.3',
        'fakeredis == 1.4.1',
    ],
    extras_require={
        'amazon': [
            'boto3 >= 1.11.9',
        ],
        'google': [
            'google-cloud-storage >= 1.10.0',
        ],
        'dev': [
            'twine',
            'moto',
            'boto3 >= 1.11.9',
            'pytest >= 3.6',
            'coverage',
            'pytest-cov',
        ]
    },
    test_suite='tests',
    entry_points={
        'servicelayer.test': [
            'test = servicelayer.extensions:get_extensions',
        ]
    }
)
