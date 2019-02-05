from setuptools import setup, find_packages


setup(
    name='servicelayer',
    version='0.1.4',
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
        'six >= 1.12',
        'banal',
        'grpcio',
        'protobuf >= 3.6.1',
        'redis >= 2.10.6, < 3',
        'fakeredis >= 1.0',
    ],
    tests_require=[
        'twine',
        'pytest',
        'coverage',
        'pytest-cov'
    ],
    test_suite='test',
    entry_points={
        'servicelayer.test': [
            'test = servicelayer.extensions:get_extensions',
        ]
    }
)
