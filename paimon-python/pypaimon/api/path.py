"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import re
import sys
import urllib.parse
from typing import Optional


class Path:
    # Static constants
    SEPARATOR = "/"
    SEPARATOR_CHAR = '/'
    CUR_DIR = "."
    WINDOWS = sys.platform.startswith("win")  # Check if running on Windows

    # Compiled regex patterns
    _has_drive_letter_specifier = re.compile(r"^/?[a-zA-Z]:")
    _slashes = re.compile(r"/+")

    def __init__(self, path_string: str):
        """Initialize Path instance from a path string"""
        # Add a slash in front of paths with Windows drive letters if needed
        if self._has_windows_drive(path_string) and path_string[0] != self.SEPARATOR_CHAR:
            path_string = self.SEPARATOR + path_string

        # Parse URI components
        scheme: Optional[str] = ""
        authority: Optional[str] = ""
        start = 0

        # Parse URI scheme if any
        colon_pos = path_string.find(':')
        slash_pos = path_string.find(self.SEPARATOR_CHAR)

        if colon_pos != -1 and (slash_pos == -1 or colon_pos < slash_pos):
            # Has a scheme
            scheme = path_string[0:colon_pos]
            start = colon_pos + 1

        # Parse URI authority if any
        if path_string.startswith("//", start) and len(path_string) - start > 2:
            # Has authority
            next_slash = path_string.find(self.SEPARATOR_CHAR, start + 2)
            auth_end = next_slash if next_slash > 0 else len(path_string)
            authority = path_string[start + 2:auth_end]
            start = auth_end

        # URI path is the rest of the string -- query & fragment not supported
        path = path_string[start:]

        self._initialize(scheme, authority, path, None)

    def _has_windows_drive(self, path_string: str) -> bool:
        """Check if the path has a Windows drive letter"""
        if not self.WINDOWS:
            return False
        return self._has_drive_letter_specifier.match(path_string) is not None

    def _initialize(self, scheme: Optional[str] = "", authority: Optional[str] = "",
                    path: Optional[str] = "", fragment: Optional[str] = ""):
        """Initialize the URI from components"""
        # Normalize slashes
        normalized_path = self._slashes.sub(self.SEPARATOR, path)

        # Construct URI string
        uri_parts: list[Optional[str]] = [scheme, authority, normalized_path, None, None, fragment]
        uri_string = urllib.parse.urlunparse(uri_parts)

        # Parse into URI object
        self.uri = urllib.parse.urlparse(uri_string)
