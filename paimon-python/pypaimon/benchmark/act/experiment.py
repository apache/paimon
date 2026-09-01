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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Load the declarative parameters shared by ACT benchmark runs."""

import json
from pathlib import Path


DEFAULT_EXPERIMENT = Path(__file__).with_name("default_experiment.json")


def load_experiment(path=None):
    """Load an ACT experiment definition from JSON or the packaged default.

    Args:
        path: Optional JSON path. When omitted, the packaged RoboMIND ACT
            benchmark defaults are loaded.

    Returns:
        A dictionary containing the benchmark identity, normalization version,
        episode selection, and shared ACT/measurement configuration.
    """
    source = DEFAULT_EXPERIMENT if path is None else Path(path)
    with source.expanduser().open(encoding="utf-8") as experiment_file:
        experiment = json.load(experiment_file)
    if not isinstance(experiment, dict):
        raise ValueError("ACT experiment must be a JSON object.")
    return experiment
