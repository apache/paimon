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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""URI path containment shared by managed BLOB readers and presigning."""

import posixpath
import re
from urllib.parse import urlparse


def normalize_uri_path(path):
    normalized = posixpath.normpath(re.sub('/+', '/', path or '/'))
    if path.startswith('/') and not normalized.startswith('/'):
        normalized = '/' + normalized
    return normalized


def table_relative_path(table_root, uri):
    """Return a normalized child path, rejecting URIs outside the table root."""
    root = urlparse(str(table_root))
    child = urlparse(str(uri))
    if ((root.scheme or '').lower() != (child.scheme or '').lower()
            or root.netloc != child.netloc):
        raise ValueError(
            "Blob descriptor URI must use the same scheme and authority "
            "as table root.")

    root_path = normalize_uri_path(root.path)
    child_path = normalize_uri_path(child.path)
    child_prefix = root_path if root_path.endswith('/') else root_path + '/'
    if not child_path.startswith(child_prefix):
        raise ValueError("Blob descriptor URI must be under table root.")
    return child_path[len(child_prefix):]
