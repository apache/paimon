##########################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
##########################################################################
from setuptools import find_packages, setup

VERSION = "0.3.dev"  # noqa

PACKAGES = find_packages(include=["pypaimon*"])

install_requires = [
    'readerwriterlock==1.0.9',
    'fsspec==2024.3.1; python_version>"3.6"',
    'fsspec==2021.10.1; python_version=="3.6"',
    'cachetools==5.3.3; python_version>"3.6"',
    'cachetools==4.2.4; python_version=="3.6"',
    'ossfs==2023.12.0; python_version>"3.6"',
    'ossfs==2021.8.0; python_version=="3.6"',
    'pyarrow>=16;   python_version >= "3.8"',
    'pyarrow==6.0.1; python_version < "3.8"',
    'pandas==2.3.2; python_version >= "3.7"',
    'pandas==1.1.5; python_version < "3.7"',
    'polars==1.32.0; python_version>"3.6"',
    'polars==0.9.12; python_version=="3.6"',
    'fastavro==1.11.1; python_version>"3.6"',
    'fastavro==1.4.7; python_version=="3.6"',
    'zstandard==0.24.0; python_version>="3.7"',
    'zstandard==0.19.0; python_version<"3.7"',
    'dataclasses==0.8.0; python_version < "3.7"',
    'pip==25.2'
]

long_description = "See Apache Paimon Python API \
[Doc](https://paimon.apache.org/docs/master/program-api/python-api/) for usage."

setup(
    name="pypaimon",
    version=VERSION,
    packages=PACKAGES,
    include_package_data=True,
    install_requires=install_requires,
    extras_require={},
    description="Apache Paimon Python API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Apache Software Foundation",
    author_email="dev@paimon.apache.org",
    url="https://paimon.apache.org",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.6",
)
