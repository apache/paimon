################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""Ray-backed compaction executor.

Driver-side: serialize each CompactTask to a JSON payload and dispatch
ray.remote tasks. Worker-side: a top-level `_run_task_payload` rebuilds
the task from the payload (which includes catalog options + table
identifier so the worker can rebuild its own FileStoreTable) and runs
it, then returns a serialized CommitMessage. The driver collects them
into Python CommitMessage objects.

`ray` is an optional dependency — installation is `pip install pypaimon[ray]`
— so we import inside execute() and present a clear error if it isn't
available.
"""

from typing import Any, Dict, List, Optional

from pypaimon.compact.executor.executor import CompactExecutor
from pypaimon.compact.task.compact_task import CompactTask
# Side-effect imports: each task subclass registers itself in the task
# registry at import time. Ray workers are fresh processes with an empty
# registry, so we must guarantee these modules get imported in the worker —
# importing them here means `import ray_executor` (which the worker does
# implicitly when unpickling _run_task_payload) brings the registrations
# along for the ride.
from pypaimon.compact.task import append_compact_task as _append_task  # noqa: F401
from pypaimon.compact.task import merge_tree_compact_task as _mt_task  # noqa: F401
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.commit_message_serializer import CommitMessageSerializer


# Worker entry point. Defined at module scope so Ray can pickle it cheaply
# and so a misbehaving subclass cannot accidentally close over driver state.
def _run_task_payload(payload: bytes) -> bytes:
    task = CompactTask.deserialize(payload)
    message = task.run()
    return CommitMessageSerializer.serialize(message)


class RayExecutor(CompactExecutor):

    def __init__(
        self,
        num_cpus_per_task: float = 1.0,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        ray_init_args: Optional[Dict[str, Any]] = None,
    ):
        """Create a RayExecutor.

        - num_cpus_per_task: per-task CPU budget; passed to ray.remote.
        - ray_remote_args: extra kwargs for the ray.remote decorator
          (e.g. {"max_retries": 3, "memory": 1<<30}).
        - ray_init_args: extra kwargs for ray.init when this executor needs
          to bootstrap Ray itself. If a Ray runtime is already initialized
          we leave it alone.
        """
        self.num_cpus_per_task = num_cpus_per_task
        self.ray_remote_args = dict(ray_remote_args or {})
        self.ray_init_args = dict(ray_init_args or {})

    def execute(self, tasks: List[CompactTask]) -> List[CommitMessage]:
        if not tasks:
            return []

        try:
            import ray  # type: ignore
        except ImportError as e:
            raise RuntimeError(
                "RayExecutor requires the 'ray' package; install pypaimon[ray] "
                "or 'pip install ray'."
            ) from e

        if not ray.is_initialized():
            ray.init(**self.ray_init_args)

        remote_run = ray.remote(num_cpus=self.num_cpus_per_task, **self.ray_remote_args)(
            _run_task_payload
        )

        # Drive serialization on the driver — gives a deterministic failure
        # site (one bad task surfaces as a TypeError here, not lost in a
        # remote traceback).
        payloads = [task.serialize() for task in tasks]
        futures = [remote_run.remote(p) for p in payloads]
        result_bytes: List[bytes] = ray.get(futures)
        return [CommitMessageSerializer.deserialize(b) for b in result_bytes]
