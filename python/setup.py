################################################################################
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
################################################################################

import os
import sys

from setuptools import setup

this_directory = os.path.abspath(os.path.dirname(__file__))
version_file = os.path.join(this_directory, 'api/version.py')

try:
    exec(open(version_file).read())
except IOError:
    print("Failed to load PyPaimon version file for packaging. " +
          "'%s' not found!" % version_file,
          file=sys.stderr)
    sys.exit(-1)
VERSION = __version__  # noqa

PACKAGES = [
    'api'
]

install_requires = [
    'py4j==0.10.9.7',
    'pandas>=1.3.0',
    'pyarrow>=5.0.0'
]

long_description = 'See Apache Paimon Python API \
[Doc](https://paimon.apache.org/docs/master/program-api/python-api/) for usage.'

setup(
    name='pypaimon_test',
    version=VERSION,
    packages=PACKAGES,
    include_package_data=True,
    install_requires=install_requires,
    extras_require={
        'avro': [
            'fastavro>=1.9.0',
            'zstandard>=0.23.0'
        ]
    },
    description='Apache Paimon Python API',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Apache Software Foundation',
    author_email='dev@paimon.apache.org',
    url='https://paimon.apache.org',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11'],
    python_requires='>=3.8'
)
