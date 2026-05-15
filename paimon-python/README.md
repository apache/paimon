![Paimon](https://github.com/apache/paimon/blob/master/docs/static/paimon-simple.png)

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

# PyPaimon

This PyPi package contains the Python APIs for using Paimon.

# Version

Pypaimon requires Python 3.6+.

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

# Inspecting query plans

`ReadBuilder.explain()` returns a structured scan plan so you can see what
a query will actually do — which snapshot it targets, what predicate /
projection / limit were pushed down, how partition / bucket / file-stats
pruning trimmed the scan, and split-level execution signals such as the
raw-convertible ratio, deletion-vector ratio, and split-size skew.

```python
read_builder = (
    table.new_read_builder()
         .with_filter(predicate)
         .with_projection(['dt', 'user_id'])
         .with_limit(1000)
)

# Compact layout
print(read_builder.explain())

# Verbose layout: also lists every split
print(read_builder.explain(verbose=True))

# Programmatic access
result = read_builder.explain()
result.split_count
result.partition_pruning            # PruningStat(before=..., after=...) or None
result.splits_raw_convertible
result.split_size_p95
```

`explain()` runs one planning pass (manifest list + manifests only — data
files are never opened).

