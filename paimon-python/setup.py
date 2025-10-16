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
import os
from setuptools import find_packages, setup

VERSION = "0.3.dev"

PACKAGES = find_packages(include=["pypaimon*"])


def read_requirements():
    """Read requirements from dev/requirements.txt file."""
    requirements_path = os.path.join(os.path.dirname(__file__), 'dev', 'requirements.txt')
    requirements = []

    if os.path.exists(requirements_path):
        with open(requirements_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                # Skip empty lines and comments
                if line and not line.startswith('#'):
                    requirements.append(line)

    return requirements


install_requires = read_requirements()

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
