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
#  limitations under the License.
################################################################################

import os
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse

from pypaimon.common.file_io import FileIO
from pypaimon.common.options.config import OssOptions


def to_lance_specified(file_io: FileIO, file_path: str) -> Tuple[str, Optional[Dict[str, str]]]:
    """Convert path and extract storage options for Lance format."""
    if hasattr(file_io, 'file_io'):
        file_io = file_io.file_io()
    
    if hasattr(file_io, 'get_merged_properties'):
        properties = file_io.get_merged_properties()
    else:
        properties = file_io.properties if hasattr(file_io, 'properties') and file_io.properties else None

    scheme, _, _ = file_io.parse_location(file_path)
    storage_options = None
    file_path_for_lance = file_io.to_filesystem_path(file_path)

    if scheme in {'file', None} or not scheme:
        if not os.path.isabs(file_path_for_lance):
            file_path_for_lance = os.path.abspath(file_path_for_lance)
    else:
        file_path_for_lance = file_path

    if scheme == 'oss':
        parsed = urlparse(file_path)
        bucket = parsed.netloc
        path = parsed.path.lstrip('/')

        if properties:
            storage_options = {}
            for key, value in properties.to_map().items():
                if str(key).startswith('fs.'):
                    storage_options[key] = value

            endpoint = properties.get(OssOptions.OSS_ENDPOINT)
            if endpoint:
                endpoint_clean = endpoint.replace('http://', '').replace('https://', '')
                storage_options['endpoint'] = f"https://{bucket}.{endpoint_clean}"

            if properties.contains(OssOptions.OSS_ACCESS_KEY_ID):
                storage_options['access_key_id'] = properties.get(OssOptions.OSS_ACCESS_KEY_ID)
                storage_options['oss_access_key_id'] = properties.get(OssOptions.OSS_ACCESS_KEY_ID)
            if properties.contains(OssOptions.OSS_ACCESS_KEY_SECRET):
                storage_options['secret_access_key'] = properties.get(OssOptions.OSS_ACCESS_KEY_SECRET)
                storage_options['oss_secret_access_key'] = properties.get(OssOptions.OSS_ACCESS_KEY_SECRET)
            if properties.contains(OssOptions.OSS_SECURITY_TOKEN):
                storage_options['session_token'] = properties.get(OssOptions.OSS_SECURITY_TOKEN)
                storage_options['oss_session_token'] = properties.get(OssOptions.OSS_SECURITY_TOKEN)
            if properties.contains(OssOptions.OSS_ENDPOINT):
                storage_options['oss_endpoint'] = properties.get(OssOptions.OSS_ENDPOINT)
            
            storage_options['virtual_hosted_style_request'] = 'true'

            if bucket and path:
                file_path_for_lance = f"oss://{bucket}/{path}"
            elif bucket:
                file_path_for_lance = f"oss://{bucket}"
        else:
            storage_options = None

    return file_path_for_lance, storage_options
