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
import shutil
import tempfile
import unittest

from pypaimon.api.catalog_factory import CatalogFactory
from pypaimon.py4j import Catalog, constants


class PypaimonTestBase(unittest.TestCase):
    """
    Base class for unit tests.
    """

    @classmethod
    def setUpClass(cls):
        os.environ[constants.PYPAIMON4J_TEST_MODE] = 'true'

        this_dir = os.path.abspath(os.path.dirname(__file__))
        project_dir = os.path.dirname(os.path.dirname(os.path.dirname(this_dir)))
        deps = os.path.join(project_dir, "dev/test_deps/*")
        os.environ[constants.PYPAIMON_HADOOP_CLASSPATH] = deps

        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = Catalog.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', False)
        cls.native_catalog = CatalogFactory.create({"warehouse": cls.warehouse})

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)
        del os.environ[constants.PYPAIMON4J_TEST_MODE]
