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

"""Resolve storage paths using Paimon's Java Path semantics.

Path text contains literal percent signs, spaces, query and fragment characters;
URL joining would incorrectly interpret or escape these characters.
"""


import os
import re


_WINDOWS = os.name == 'nt'


def _has_drive(path):
    return bool(re.match(r'^/?[A-Za-z]:', path))


def _normalize(path):
    components = []
    for component in path.split('/'):
        if component in ('', '.'):
            continue
        if component == '..' and components and components[-1] != '..':
            components.pop()
        else:
            components.append(component)
    normalized = ('/' if path.startswith('/') else '') + '/'.join(components)
    if (_WINDOWS and normalized.startswith('/') and _has_drive(normalized)
            and len(normalized) == 3 and path != normalized):
        normalized += '/'
    elif not normalized.startswith('/') and ':' in normalized.split('/')[0]:
        normalized = './' + normalized
    return normalized


def _parts(path):
    if _WINDOWS and _has_drive(path) and not path.startswith('/'):
        path = '/' + path
    colon = path.find(':')
    scheme_end = colon + 1 if colon >= 0 and '/' not in path[:colon] else 0
    scheme, authority = path[:scheme_end], ''
    if path[scheme_end:].startswith('//') and len(path) - scheme_end > 2:
        slash = path.find('/', scheme_end + 2)
        end = len(path) if slash < 0 else slash
        if end > scheme_end + 2:
            authority = path[scheme_end:end]
        path = path[end:]
    else:
        path = path[scheme_end:]
    path = re.sub(r'/+', '/', path)
    if _WINDOWS and (_has_drive(path) or scheme in ('', 'file:')):
        path = path.replace('\\', '/')
    # Java URI recognizes a UNC authority after Windows separator conversion.
    if not authority and path.startswith('//') and len(path) > 2:
        end = path.find('/', 2)
        end = len(path) if end < 0 else end
        authority, path = path[:end], path[end:]
    return scheme, authority, _normalize(path)


def resolve_path(parent, child):
    """Resolve child against a directory, retaining its scheme and authority."""
    if child is None:
        return parent.rstrip('/') or '/'
    if child == '':
        raise ValueError('Can not create a Path from an empty string')
    parent_scheme, parent_authority, parent_path = _parts(parent)
    child_scheme, child_authority, child_path = _parts(child)
    if child_scheme:
        scheme, authority, path = child_scheme, child_authority, child_path
    elif child_authority:
        scheme, authority, path = parent_scheme, child_authority, child_path
    elif child_path.startswith('/'):
        scheme, authority, path = parent_scheme, parent_authority, child_path
    else:
        scheme, authority = parent_scheme, parent_authority
        path = (parent_path.rstrip('/') + '/' + child_path
                if parent_path or (child_path and (scheme or authority)) else child_path)
    path = _normalize(path)
    if not scheme and not authority:
        if _WINDOWS and _has_drive(path):
            path = path.lstrip('/')
        elif not path.startswith('/') and ':' in path.split('/')[0]:
            path = './' + path
    return scheme + authority + path


def to_file_io_path(path):
    """Keep literal Java file-path characters from being decoded as a URL."""
    if not path.startswith('file:') or not any(char in path for char in '%?#'):
        return path
    _, authority, local_path = _parts(path)
    if authority and authority != '//localhost':
        if not (_WINDOWS and authority.endswith(':')):
            return authority + local_path
        local_path = authority[1:] + local_path
    if _WINDOWS and _has_drive(local_path):
        local_path = local_path.lstrip('/')
    return local_path
