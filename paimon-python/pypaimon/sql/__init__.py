# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

__all__ = ['SQLContext', 'register_builtins', 'register_first_frame']


def __getattr__(name):
    if name == "SQLContext":
        from pypaimon_rust.datafusion import SQLContext
        return SQLContext
    if name == "register_builtins":
        from pypaimon.sql.functions import register_builtins
        return register_builtins
    if name == "register_first_frame":
        from pypaimon.sql.functions import register_first_frame
        return register_first_frame
    raise AttributeError("module 'pypaimon.sql' has no attribute {}".format(name))
