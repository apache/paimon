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

"""Compatibility with old and public Rust builder APIs."""

import unittest
from unittest.mock import Mock

from pypaimon.write.native_commit import with_native_commit_user


class NativeBuilderCompatTest(unittest.TestCase):

    def test_public_api_is_preferred(self):
        builder = Mock(spec=['with_commit_user', '_with_commit_user'])
        result = with_native_commit_user(builder, 'writer-id')
        self.assertIs(result, builder.with_commit_user.return_value)
        builder.with_commit_user.assert_called_once_with('writer-id')
        builder._with_commit_user.assert_not_called()

    def test_legacy_api_is_supported(self):
        builder = Mock(spec=['_with_commit_user'])
        result = with_native_commit_user(builder, 'writer-id')
        self.assertIs(result, builder._with_commit_user.return_value)
        builder._with_commit_user.assert_called_once_with('writer-id')

    def test_public_api_errors_are_not_hidden_by_legacy_fallback(self):
        builder = Mock(spec=['with_commit_user', '_with_commit_user'])
        builder.with_commit_user.side_effect = AttributeError('failure inside builder')
        with self.assertRaisesRegex(AttributeError, 'failure inside builder'):
            with_native_commit_user(builder, 'writer-id')
        builder._with_commit_user.assert_not_called()
