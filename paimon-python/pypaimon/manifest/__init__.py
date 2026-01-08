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

# Apply fastavro Python 3.6 compatibility patch early, before any other
# manifest modules are imported that might use fastavro
import sys
if sys.version_info[:2] == (3, 6):
    try:
        # Import fastavro_py36_compat module (this file is in the same directory)
        from pypaimon.manifest import fastavro_py36_compat  # noqa: F401
        if fastavro_py36_compat is not None:
            fastavro_py36_compat._apply_zstd_patch()
    except (ImportError, AttributeError, NameError):
        # Module may not be available in some environments, silently skip
        pass
