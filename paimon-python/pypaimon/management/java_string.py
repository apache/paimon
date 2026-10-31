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
from typing import Optional


# Character.isWhitespace since Java 9 (Java 8 also takes U+180E): unlike str.isspace, the
# no-break spaces and NEL are not whitespace.
_JAVA_WHITESPACE = frozenset(
    '\t\n\v\f\r\x1c\x1d\x1e\x1f \u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006'
    '\u2008\u2009\u200a\u2028\u2029\u205f\u3000')


def is_blank(value: Optional[str]) -> bool:
    """Java ``trim().isEmpty()``: only characters <= U+0020 are blank, unlike ``str.strip()``."""
    return value is None or all(ch <= ' ' for ch in value)


def is_null_or_whitespace_only(value: Optional[str]) -> bool:
    """Java ``StringUtils.isNullOrWhitespaceOnly``, unlike ``str.strip()``."""
    return value is None or all(ch in _JAVA_WHITESPACE for ch in value)


def java_length(value: str) -> int:
    """Java ``String.length()``: UTF-16 code units, not code points."""
    return len(value.encode('utf-16-le', 'surrogatepass')) // 2


def utf8_length(value: str) -> int:
    """Java ``getBytes(UTF_8).length``: an unpaired surrogate counts as one byte."""
    return len(value.encode('utf-8', 'replace'))
