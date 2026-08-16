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

"""Shared Kerberos helpers used by HDFS FileIO backends."""

import os
import subprocess
from typing import Optional


def kerberos_login_from_keytab(principal: str, keytab: str) -> None:
    if not os.path.isfile(keytab):
        raise FileNotFoundError(f"Kerberos keytab file not found: {keytab}")
    if not os.access(keytab, os.R_OK):
        raise PermissionError(f"Kerberos keytab file is not readable: {keytab}")
    subprocess.run(
        ['kinit', '-kt', keytab, principal],
        check=True, capture_output=True, text=True,
    )


def get_ticket_cache_path() -> Optional[str]:
    cc = os.environ.get('KRB5CCNAME')
    if cc:
        if cc.startswith('FILE:'):
            return cc[5:]
        return cc
    default_path = f'/tmp/krb5cc_{os.getuid()}'
    if os.path.exists(default_path):
        return default_path
    return None
