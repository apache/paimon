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

import unittest
import requests
from requests.exceptions import ConnectionError, ConnectTimeout, Timeout
from urllib3.exceptions import NewConnectionError, MaxRetryError

from pypaimon.api.client import ExponentialRetry


class TestExponentialRetryStrategy(unittest.TestCase):

    def setUp(self):
        self.retry_strategy = ExponentialRetry(max_retries=5)

    def test_basic_retry(self):
        retry = ExponentialRetry._ExponentialRetry__create_retry_strategy(5)
        
        self.assertEqual(retry.total, 5)
        self.assertEqual(retry.read, 5)
        self.assertEqual(retry.connect, 0)  # Connection errors should not retry
        
        self.assertIn(429, retry.status_forcelist)  # Too Many Requests
        self.assertIn(503, retry.status_forcelist)  # Service Unavailable
        self.assertNotIn(404, retry.status_forcelist)

    def test_no_retry_on_connect_error(self):
        session = requests.Session()
        session.mount("http://", self.retry_strategy.adapter)
        session.mount("https://", self.retry_strategy.adapter)
        session.timeout = (1, 1)

        import time
        start_time = time.time()
        
        try:
            session.get("http://192.168.255.255:9999", timeout=(1, 1))
            self.fail("Expected ConnectionError")
        except (ConnectionError, ConnectTimeout, Timeout, NewConnectionError, MaxRetryError) as e:
            elapsed = time.time() - start_time
            self.assertLess(
                elapsed, 5.0,
                f"Connection error took {elapsed:.2f}s, should fail quickly without retry"
            )


if __name__ == '__main__':
    unittest.main()

