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

import random
import time
from datetime import timedelta
from typing import Callable, Optional

from pypaimon.catalog.prediction import Prediction
from pypaimon.common.wait import Wait


class PredictionWait(Wait[Prediction]):
    """Wait handle returned after submitting an asynchronous prediction."""

    def __init__(self, catalog, prediction: Prediction):
        super().__init__(
            lambda prediction_id, timeout, callback: _wait_for_prediction(
                catalog, prediction_id, timeout, callback),
            response=prediction,
            prediction_id=prediction.prediction_id,
        )


def _wait_for_prediction(
        catalog,
        prediction_id: str,
        timeout: timedelta,
        callback: Optional[Callable[[Prediction], None]],
) -> Prediction:
    if not isinstance(timeout, timedelta):
        raise ValueError("timeout must be a datetime.timedelta")
    if timeout.total_seconds() <= 0:
        raise ValueError("timeout must be positive")

    deadline = time.monotonic() + timeout.total_seconds()
    attempt = 1
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        try:
            prediction = catalog.get_prediction(
                prediction_id, timeout_seconds=remaining)
        except TimeoutError as e:
            raise TimeoutError(
                "Prediction {} did not finish within {}".format(
                    prediction_id, timeout)) from e
        if time.monotonic() >= deadline:
            break
        if prediction.status.terminal:
            return prediction
        if callback is not None:
            callback(prediction)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        sleep_seconds = min(attempt, 10) + random.random()
        time.sleep(min(sleep_seconds, remaining))
        attempt += 1

    raise TimeoutError(
        "Prediction {} did not finish within {}".format(
            prediction_id, timeout))
