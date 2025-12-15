![Paimon](https://github.com/apache/paimon/blob/master/docs/static/paimon-simple.png)

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

# PyPaimon

This PyPi package contains the Python APIs for using Paimon.

# Version

Pypaimon requires Python 3.10+ version.

# Dependencies

The core dependencies are listed in `dev/requirements.txt`.
The development dependencies are listed in `dev/requirements-dev.txt`.

# Build

You can build the source package by executing the following command:

```commandline
python3 setup.py sdist
```

The package is under `dist/`. Then you can install the package by executing the following command:

```commandline
pip3 install dist/*.tar.gz
```

The command will install the package and core dependencies to your local Python environment.

