![Paimon](https://github.com/apache/paimon/blob/master/docs/static/paimon-simple.png)

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

# PyPaimon

This PyPi package contains the Python APIs for using Paimon.

# Version

Pypaimon requires Python 3.10+ version.

# Dependencies

Be building, you should install python setuptools:

```commandline
brew install python-setuptools
pip3 install readerwriterlock
pip3 install fsspec
pip3 install cachetools
pip3 install ossfs
pip3 install pyarrow
pip3 install polars
pip3 install fastavro
pip3 install pandas
pip3 install ray
pip3 install duckdb
```

# Build

You can build the source package by executing the following command:

```commandline
python3 setup.py sdist
```

The package is under `dist/`.
