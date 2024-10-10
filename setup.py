"""
Data Utilities.
"""

# To use a consistent encoding
import codecs
from os import path

# Always prefer setuptools over distutils
from setuptools import setup

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with codecs.open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='data-utils',

    # Versions should comply with PEP440. For a discussion on single-sourcing
    # the version across setup.py and the project code, see
    # https://packaging.python.org/en/latest/single_source_version.html
    version='0.1.45',
    description='Data Utilities.',
    long_description=long_description,
    long_description_content_type='text/markdown',

    # The project's main homepage.
    url='https://github.com/owner/data-utils',

    # Author details
    author='Lucas Bulgarelli',
    author_email='lucas.bulgarelli@hotmail.com',

    # Choose your license
    license='MIT',

    # What does your project relate to?
    keywords='database utilities',

    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    # packages=find_packages(exclude=['contrib', 'docs', 'tests']),
    packages=["data_utils"],

    # Alternatively, if you want to distribute just a my_module.py, uncomment
    # this:
    # py_modules=['data_utils'],

    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed. For an analysis of "install_requires" vs pip's
    # requirements files see:
    # https://packaging.python.org/en/latest/requirements.html
    install_requires=[
        "pandas>=2.1.0", "openpyxl>=3.1.0", "python-slugify>=8.0.0",
        "sqlalchemy>=2.0.0", "python-decouple>=3.8", "pytest>=8.1",
        "psycopg2-binary>=2.9", "pycap>=2.6.0", "sqlalchemy-utils>=0.41.2"
    ],

    # Add to site-packages
    entry_points={
        "console_scripts": [
            'build_json = data_utils.build_json:main',
            'build_table = data_utils.build_table:main',
            'build_database = data_utils.build_database:main',
            'build_dbms = data_utils.build_dbms:main'
        ]
    },
)
