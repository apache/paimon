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

import logging

import pytest

from pypaimon.read import table_scan


TOKEN_DNS_ERROR = RuntimeError(
    'http get failed: reqwest::Error { url: '
    '"https://dlf.example/v1/db/tables/t/token", '
    'source: ConnectError("dns error", '
    '"failed to lookup address information") }')


def test_retries_only_token_dns(monkeypatch, caplog):
    waits = []
    monkeypatch.setattr(table_scan.time, 'sleep', waits.append)
    monkeypatch.setattr(table_scan.random, 'uniform', lambda _low, _high: 0)
    calls = 0

    def plan():
        nonlocal calls
        calls += 1
        if calls < 3:
            raise TOKEN_DNS_ERROR
        return 'planned'

    with caplog.at_level(logging.WARNING, logger=table_scan.__name__):
        assert table_scan._retry_native_token_dns(plan) == 'planned'
    assert calls == 3
    assert waits == [0.1, 0.3]
    assert [record.levelno for record in caplog.records] == [
        logging.WARNING, logging.WARNING]
    assert 'attempt 1/4' in caplog.records[0].message
    assert 'attempt 2/4' in caplog.records[1].message


@pytest.mark.parametrize('error', [
    RuntimeError('http get failed: dns error on /manifest'),
    RuntimeError('http get failed: 503 on /token'),
    RuntimeError('parquet decode failed'),
])
def test_other_errors_are_not_retried(monkeypatch, error):
    monkeypatch.setattr(
        table_scan.time, 'sleep', lambda _delay: pytest.fail('slept'))
    calls = 0

    def plan():
        nonlocal calls
        calls += 1
        raise error

    with pytest.raises(type(error)):
        table_scan._retry_native_token_dns(plan)
    assert calls == 1


def test_retry_is_bounded(monkeypatch, caplog):
    monkeypatch.setattr(table_scan.time, 'sleep', lambda _delay: None)
    monkeypatch.setattr(table_scan.random, 'uniform', lambda _low, _high: 0)
    calls = 0

    def plan():
        nonlocal calls
        calls += 1
        raise TOKEN_DNS_ERROR

    with caplog.at_level(logging.WARNING, logger=table_scan.__name__):
        with pytest.raises(RuntimeError):
            table_scan._retry_native_token_dns(plan)
    assert calls == 4
    assert [record.levelno for record in caplog.records] == [logging.WARNING] * 4
    assert 'retries exhausted' in caplog.records[-1].message
