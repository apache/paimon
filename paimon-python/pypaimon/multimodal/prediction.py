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

from pypaimon.catalog.prediction import Prediction
from pypaimon.catalog.prediction import PredictionTerminationCode
from pypaimon.common.wait import Wait


class PredictionFailedError(RuntimeError):
    """Raised when an asynchronous prediction fails."""

    def __init__(self, prediction: Prediction):
        details = prediction.status.termination_details
        message = details.message if details is not None else None
        super().__init__(
            message or "Prediction {} failed".format(
                prediction.prediction_id))
        self.prediction = prediction


class PredictionCanceledError(RuntimeError):
    """Raised when an asynchronous prediction is canceled."""

    def __init__(self, prediction: Prediction):
        details = prediction.status.termination_details
        message = details.message if details is not None else None
        super().__init__(
            message or "Prediction {} was canceled".format(
                prediction.prediction_id))
        self.prediction = prediction


class PredictionWait(Wait[Prediction]):
    """Wait handle returned after submitting an asynchronous prediction."""

    def __init__(self, catalog, prediction: Prediction):
        self._catalog = catalog
        prediction_id = prediction.prediction_id
        super().__init__(
            prediction,
            lambda: catalog.get_prediction(prediction_id),
            lambda value: value.status.terminal,
            _validate_prediction_result,
            "Prediction {}".format(prediction_id),
        )

    @property
    def prediction_id(self) -> str:
        return self.current.prediction_id

    def cancel(self) -> 'PredictionWait':
        self._current = self._catalog.cancel_prediction(self.prediction_id)
        return self


def _validate_prediction_result(prediction: Prediction) -> Prediction:
    details = prediction.status.termination_details
    if details is None:
        raise RuntimeError(
            "Terminal prediction {} has no termination details".format(
                prediction.prediction_id))
    if details.code is PredictionTerminationCode.FAILED:
        raise PredictionFailedError(prediction)
    if details.code is PredictionTerminationCode.CANCELED:
        raise PredictionCanceledError(prediction)
    return prediction
