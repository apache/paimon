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
from datetime import timedelta
from typing import Optional

from pypaimon.catalog.prediction import Prediction
from pypaimon.common.identifier import Identifier


class PredictionJob:
    """Client handle for a durable asynchronous table prediction."""

    def __init__(
            self, catalog, identifier: Identifier, prediction: Prediction):
        self._catalog = catalog
        self._identifier = identifier
        self._prediction = prediction

    @property
    def prediction(self) -> Prediction:
        return self._prediction

    def __getattr__(self, name):
        return getattr(self._prediction, name)

    def refresh(self) -> 'PredictionJob':
        self._prediction = self._catalog.get_prediction(
            self._identifier, self._prediction.job_id)
        return self

    def wait(
            self,
            timeout: Optional[timedelta] = None,
            poll_interval: float = 1.0,
    ) -> 'PredictionJob':
        """Poll prediction metadata until it reaches a terminal state.

        This is client-side polling over ``get_prediction``. It does not keep
        a long-lived request open on the REST service.
        """
        if timeout is not None and not isinstance(timeout, timedelta):
            raise ValueError("timeout must be a datetime.timedelta")
        if poll_interval < 0:
            raise ValueError("poll_interval must not be negative")
        deadline = (
            None if timeout is None
            else time.monotonic() + timeout.total_seconds()
        )
        while not self._prediction.status.terminal:
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        "Prediction {} did not finish within {}".format(
                            self._prediction.job_id, timeout))
                time.sleep(min(poll_interval, remaining))
            else:
                time.sleep(poll_interval)
            self.refresh()
        return self

    def cancel(self) -> 'PredictionJob':
        self._prediction = self._catalog.cancel_prediction(
            self._identifier, self._prediction.job_id)
        return self
