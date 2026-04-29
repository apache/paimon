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

import time
import unittest
import requests
from requests.exceptions import ConnectionError, ConnectTimeout, Timeout
from urllib3.exceptions import NewConnectionError, MaxRetryError

from pypaimon.api.client import ExponentialRetry


class TestExponentialRetryStrategy(unittest.TestCase):

    def test_basic_retry(self):
        retry = ExponentialRetry._ExponentialRetry__create_retry_strategy(5, 3)

        self.assertIsNone(retry.total)
        self.assertEqual(retry.connect, 5)
        self.assertEqual(retry.read, 3)
        self.assertEqual(retry.status, 3)

        self.assertIn(429, retry.status_forcelist)  # Too Many Requests
        self.assertIn(503, retry.status_forcelist)  # Service Unavailable
        self.assertNotIn(404, retry.status_forcelist)

    def test_retry_on_connect_error(self):
        retry_strategy = ExponentialRetry(connect_retries=2, read_retries=0)
        session = requests.Session()
        session.mount("http://", retry_strategy.adapter)
        session.mount("https://", retry_strategy.adapter)

        start_time = time.time()

        try:
            session.get("http://192.168.255.255:9999", timeout=(1, 1))
            self.fail("Expected ConnectionError")
        except (ConnectionError, ConnectTimeout, Timeout, NewConnectionError, MaxRetryError):
            elapsed = time.time() - start_time
            # connect_retries=2 with backoff_factor=1 → at least 1s of backoff between attempts
            self.assertGreaterEqual(
                elapsed, 2.0,
                f"Connection error took {elapsed:.2f}s, expected retries with backoff"
            )

    def test_no_connect_retry_but_read_retry(self):
        retry = ExponentialRetry._ExponentialRetry__create_retry_strategy(0, 5)
        self.assertEqual(retry.connect, 0)
        self.assertEqual(retry.read, 5)
        self.assertEqual(retry.status, 5)

    def test_zero_retries(self):
        retry = ExponentialRetry._ExponentialRetry__create_retry_strategy(0, 0)
        self.assertEqual(retry.connect, 0)
        self.assertEqual(retry.read, 0)
        self.assertEqual(retry.status, 0)


if __name__ == '__main__':
    unittest.main()
