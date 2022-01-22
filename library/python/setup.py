from glob import glob
from os.path import splitext
from os.path import basename

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="dc",
    version="1.0.2",
    url="https://github.com/DutchChannels/analytics-tier.git",
    author="Dutchchannels Data Team",
    author_email="data@dutchchannels.nl",
    description="Python functions written for DutchChannels databricks cluster.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    packages=find_packages('src/lib'),
    package_dir={'': 'src/lib'},
    py_modules=[splitext(basename(path))[0] for path in glob('src/lib/*.py')],
    include_package_data=True,
    zip_safe=False,
    python_requires=">=3.6",
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
    test_suite="tests",
    install_requires=[
        'azure-cosmos==4.2.0',
        'pandas==1.1.3',
        'pyspark==3.1.2',
        'pyodbc==4.0.32',
        'sqlalchemy==1.4.23',
        'pymssql==2.2.2',
        'scipy==1.7.3'
    ]
)
