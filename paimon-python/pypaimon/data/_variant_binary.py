################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
################################################################################

"""Shared Variant binary constants and low-level helpers.

Extracted from generic_variant.py and variant_shredding.py to avoid duplication.
All symbols here mirror the Parquet Variant binary specification and
GenericVariantUtil.java.
"""

# ---------------------------------------------------------------------------
# Basic type tags (2-bit field in the header byte)
# ---------------------------------------------------------------------------

_PRIMITIVE = 0
_SHORT_STR = 1
_OBJECT = 2
_ARRAY = 3

# ---------------------------------------------------------------------------
# Integer size limits used for offset / size encoding
# ---------------------------------------------------------------------------

_U8_MAX = 255
_U16_MAX = 65535
_U24_MAX = 16777215
_U32_SIZE = 4

# ---------------------------------------------------------------------------
# Metadata versioning
# ---------------------------------------------------------------------------

_VERSION = 1
_VERSION_MASK = 0x0F


# ---------------------------------------------------------------------------
# Low-level binary helpers
# ---------------------------------------------------------------------------

def _read_unsigned(data, pos, n):
    return int.from_bytes(data[pos:pos + n], 'little', signed=False)


def _get_int_size(value):
    if value <= _U8_MAX:
        return 1
    if value <= _U16_MAX:
        return 2
    if value <= _U24_MAX:
        return 3
    return 4


def _primitive_header(type_id):
    return (type_id << 2) | _PRIMITIVE


def _object_header(large_size, id_size, offset_size):
    return (
        ((1 if large_size else 0) << 6)
        | ((id_size - 1) << 4)
        | ((offset_size - 1) << 2)
        | _OBJECT
    )


def _array_header(large_size, offset_size):
    return (
        ((1 if large_size else 0) << 4)
        | ((offset_size - 1) << 2)
        | _ARRAY
    )
