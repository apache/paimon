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

from concurrent.futures import Future
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
from pypaimon.table.source.full_text_read import DataEvolutionFullTextRead
from pypaimon.tests.vector_search_filter_test import _StubTable, _entry, _field


@pytest.mark.parametrize("mode", ["sync_error", "sync_interrupt", "completed", "pending", "failed"])
def test_full_text_reader_lifetime(mode):
    field = _field(0, "text", "STRING")
    table = _StubTable([field], [])
    entry = _entry(None, 0, "full-text", "index", 10, 19)
    native = Mock()
    future = Future()
    failure = KeyboardInterrupt() if mode == "sync_interrupt" else ValueError("search failed")
    if mode.startswith("sync_"):
        native.visit_full_text_search.side_effect = failure
    else:
        native.visit_full_text_search.return_value = future
        if mode == "completed":
            future.set_result(DictBasedScoredIndexResult({1: 0.5}))

    with patch("pypaimon.table.source.full_text_read._create_full_text_reader", return_value=native):
        read = DataEvolutionFullTextRead(table, 1, field, "query")
        if mode.startswith("sync_"):
            with pytest.raises(type(failure)) as exc:
                read._eval(10, 19, [entry.index_file], None)
            assert exc.value is failure
        else:
            result = read._eval(10, 19, [entry.index_file], None)
            if mode != "completed":
                native.close.assert_not_called()
                assert not result.done()
                if mode == "failed":
                    future.set_exception(failure)
                else:
                    future.set_result(DictBasedScoredIndexResult({1: 0.5}))
            if mode == "failed":
                with pytest.raises(ValueError) as exc:
                    result.result()
                assert exc.value is failure
            else:
                assert list(result.result().results()) == [11]
    native.close.assert_called_once_with()


@pytest.mark.parametrize("failure_type", [ValueError, KeyboardInterrupt, SystemExit])
def test_full_text_reader_closes_stream_when_native_loading_fails(failure_type):
    field = _field(0, "text", "STRING")
    table = _StubTable([field], [])
    entry = _entry(None, 0, "full-text", "index", 10, 19)
    stream = BytesIO(b"index")
    table.file_io = Mock()
    table.file_io.new_input_stream.return_value = stream
    failure = failure_type("native reader construction failed")
    native_module = SimpleNamespace(FullTextIndexReader=Mock(side_effect=failure))

    with patch.dict("sys.modules", {"paimon_ftindex": native_module}):
        read = DataEvolutionFullTextRead(table, 1, field, "query")
        with pytest.raises(failure_type) as exc:
            read._eval(10, 19, [entry.index_file], None)

    assert exc.value is failure
    assert stream.closed
