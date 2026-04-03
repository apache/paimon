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
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse

from pypaimon.common.file_io import FileIO
from pypaimon.common.options.config import OssOptions


def to_vortex_specified(file_io: FileIO, file_path: str) -> Tuple[str, Optional[Dict[str, str]]]:
    """Convert path and extract storage options for Vortex store.from_url().

    Returns (url, store_kwargs) where store_kwargs can be passed as
    keyword arguments to ``vortex.store.from_url(url, **store_kwargs)``.
    For local paths store_kwargs is None.
    """
    if hasattr(file_io, 'file_io'):
        file_io = file_io.file_io()

    if hasattr(file_io, 'get_merged_properties'):
        properties = file_io.get_merged_properties()
    else:
        properties = file_io.properties if hasattr(file_io, 'properties') and file_io.properties else None

    scheme, _, _ = file_io.parse_location(file_path)
    file_path_for_vortex = file_io.to_filesystem_path(file_path)

    store_kwargs = None

    if scheme in {'file', None} or not scheme:
        if not os.path.isabs(file_path_for_vortex):
            file_path_for_vortex = os.path.abspath(file_path_for_vortex)
        return file_path_for_vortex, None

    # For remote schemes, keep the original URI so vortex can parse it
    file_path_for_vortex = file_path

    if scheme == 'oss' and properties:
        parsed = urlparse(file_path)
        bucket = parsed.netloc

        store_kwargs = {
            'endpoint': f"https://{bucket}.{properties.get(OssOptions.OSS_ENDPOINT)}",
            'access_key_id': properties.get(OssOptions.OSS_ACCESS_KEY_ID),
            'secret_access_key': properties.get(OssOptions.OSS_ACCESS_KEY_SECRET),
            'virtual_hosted_style_request': 'true',
        }
        if properties.contains(OssOptions.OSS_SECURITY_TOKEN):
            store_kwargs['session_token'] = properties.get(OssOptions.OSS_SECURITY_TOKEN)

        file_path_for_vortex = file_path_for_vortex.replace('oss://', 's3://')

    return file_path_for_vortex, store_kwargs
