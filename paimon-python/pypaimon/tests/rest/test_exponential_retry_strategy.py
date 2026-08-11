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

import time
import unittest
import requests
from requests.exceptions import ConnectionError, ConnectTimeout, Timeout
from urllib3.exceptions import NewConnectionError, MaxRetryError
from urllib3.util.retry import RequestHistory

from pypaimon.api.client import ExponentialBackoffRetry, ExponentialRetry


class TestExponentialRetryStrategy(unittest.TestCase):

    def test_basic_retry(self):
        retry = ExponentialRetry._ExponentialRetry__create_retry_strategy(5)

        self.assertEqual(retry.total, 5)
        # Read errors / timeouts are not retried: the request has likely
        # reached the server and its signature nonce is already consumed,
        # so a retry with the same signed headers would be rejected with
        # "Specified signature nonce was used already".
        self.assertEqual(retry.read, 0)
        # Connect failures are intentionally non-retriable — see the
        # comment on ``ExponentialRetry.__create_retry_strategy``.
        self.assertEqual(retry.connect, 0)
        self.assertEqual(retry.status, 5)

        # Aligned with the Java client: only 429 / 503 are retried.
        self.assertIn(429, retry.status_forcelist)  # Too Many Requests
        self.assertIn(503, retry.status_forcelist)  # Service Unavailable
        self.assertNotIn(404, retry.status_forcelist)
        self.assertNotIn(502, retry.status_forcelist)
        self.assertNotIn(504, retry.status_forcelist)

        self.assertIsInstance(retry, ExponentialBackoffRetry)

    def test_backoff_schedule_matches_java(self):
        # Java ExponentialHttpRequestRetryStrategy sleeps
        # 1000 * min(2^(execCount-1), 64) ms plus up to 10% jitter
        # after each failed attempt.
        base = ExponentialRetry._ExponentialRetry__create_retry_strategy(5)

        def backoff_after(failures):
            history = tuple(
                RequestHistory("GET", "http://host", None, 503, None)
                for _ in range(failures))
            return base.new(history=history).get_backoff_time()

        expected = [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 64.0]
        for failures, base_delay in enumerate(expected, start=1):
            value = backoff_after(failures)
            self.assertGreaterEqual(
                value, base_delay,
                "backoff after {} failure(s) must be >= {}s".format(failures, base_delay))
            self.assertLessEqual(
                value, base_delay * 1.1 + 1e-9,
                "backoff after {} failure(s) must be <= {}s + 10% jitter".format(
                    failures, base_delay))

    def test_retry_on_connect_error(self):
        # ``connect=0`` means connect errors are not retried — the
        # request should fail fast within roughly the connect timeout.
        retry_strategy = ExponentialRetry(max_retries=2)
        session = requests.Session()
        session.mount("http://", retry_strategy.adapter)
        session.mount("https://", retry_strategy.adapter)

        start_time = time.time()

        try:
            session.get("http://192.168.255.255:9999", timeout=(1, 1))
            self.fail("Expected ConnectionError")
        except (ConnectionError, ConnectTimeout, Timeout, NewConnectionError, MaxRetryError):
            elapsed = time.time() - start_time
            # No connect retries → bail out within roughly the connect
            # timeout, with no exponential backoff.
            self.assertLess(
                elapsed, 5.0,
                "connect failures should not be retried (got {:.2f}s)".format(elapsed)
            )


if __name__ == '__main__':
    unittest.main()
