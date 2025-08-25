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
import tempfile
import unittest

import pyarrow as pa

from pypaimon.catalog.catalog_factory import CatalogFactory
from pypaimon.schema.schema import Schema
from pypaimon.tests.py4j_impl.java_implementation import CatalogPy4j
from pypaimon.tests.py4j_impl import constants

if __name__ == '__main__':

    # for py4j env
    this_dir = os.path.abspath(os.path.dirname(__file__))
    project_dir = os.path.dirname(this_dir)
    deps = os.path.join(project_dir, "tests/py4j_impl/test_deps/*")
    os.environ[constants.PYPAIMON_HADOOP_CLASSPATH] = deps
    os.environ[constants.PYPAIMON4J_TEST_MODE] = 'true'

    # java pk表
    # /var/folders/df/wn6fl15j37z40yw877brfq8m0000gp/T/tmp3juz9qrr/warehouse/default.db/test_alternative_write/snapshot/snapshot-8
    # java append表
    # /var/folders/df/wn6fl15j37z40yw877brfq8m0000gp/T/tmphoxj21h3/warehouse/default.db/test_alternative_write/manifest/
    # warehouse = "/var/folders/df/wn6fl15j37z40yw877brfq8m0000gp/T/tmpxpwzougv/warehouse"
    warehouse = "/var/folders/df/wn6fl15j37z40yw877brfq8m0000gp/T/tmpjmwlp7q2/warehouse"
    option = {
        'warehouse': warehouse
    }
    py_catalog = CatalogFactory.create(option)
    py_table = py_catalog.get_table('default.test_alternative_write')
    py_read_builder = py_table.new_read_builder()
    py_table_read = py_read_builder.new_read()
    py_splits = py_read_builder.new_scan().plan().splits()
    py_actual = py_table_read.to_arrow(py_splits)
    print(py_actual)
