# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from types import SimpleNamespace

from pypaimon.execution.python.read.scan_distribution import shard_range
from pypaimon.multimodal.columns import _prepare_column_shard
from pypaimon.read.read_support import close_read_source
from pypaimon.write.backend_dispatch import create_python_shard_updator
from pypaimon.write.row_id_plan import capture_row_id_plan
from pypaimon.write.update_validation import _authorize_update


class _ShardError(RuntimeError):
    def __init__(self, phase, error):
        self.phase = phase
        self.error = error
        # Ray pickles the exception arguments, but not its cause chain.
        super().__init__(phase, error)


def validate_ray_options(concurrency, ray_remote_args):
    concurrency = 4 if concurrency is None else concurrency
    if isinstance(concurrency, bool) or not isinstance(concurrency, int) or concurrency < 1:
        raise ValueError("concurrency must be a positive integer.")
    options = dict(ray_remote_args or {})
    if "num_returns" in options:
        raise ValueError("compute_columns manages num_returns; omit it from ray_remote_args.")
    if options.get("max_retries", 0) != 0 or options.get("retry_exceptions", False):
        raise ValueError("Ray column computation requires max_retries=0 and retry_exceptions=False.")
    options.update(max_retries=0, retry_exceptions=False)
    try:
        import ray  # noqa: F401
    except ModuleNotFoundError as error:
        if error.name != "ray":
            raise
        raise ImportError("Ray column computation requires pypaimon[ray].") from error
    return concurrency, options


def prepare_ray_column_update(operation, table, commit_user, inputs, schema, fn, batch_size, concurrency, remote_args):
    import ray
    from ray.exceptions import RayTaskError

    plan = capture_row_id_plan(table)
    groups = [split for _, _, split in plan.groups]
    count = min(len(groups), concurrency)
    if not count:
        return [], 0

    context = ray.put((plan.table, commit_user, inputs, schema, fn, batch_size))
    # Ray sets the default GPU worker exit policy when registering the function.
    worker = ray.remote(**remote_args)(_prepare_shard)
    pending = []
    try:
        try:
            for index in range(count):
                start, end = shard_range(len(groups), index, count)
                pending.append(worker.remote(context, groups[start:end]))
            results = ray.get(pending)
        except Exception as error:
            # Drain dispatched writers and retain their staged files.
            for ref in pending:
                try:
                    ray.get(ref)
                except Exception:
                    pass
            if isinstance(error, RayTaskError) and isinstance(error.cause, _ShardError):
                operation.phase = error.cause.phase
                raise error.cause.error from error
            raise
    except KeyboardInterrupt:
        # Cancel later submissions first so they cannot use slots freed by earlier tasks.
        for ref in reversed(pending):
            try:
                ray.cancel(ref)
            except Exception:
                pass
        raise
    return [message for messages, _ in results for message in messages], sum(rows for _, rows in results)


def _prepare_shard(context, splits):
    table, commit_user, inputs, schema, fn, batch_size = context
    _authorize_update(table)
    shard = create_python_shard_updator(table, inputs, schema.names, commit_user, splits=splits)
    progress = SimpleNamespace(phase="compute")
    try:
        result = _prepare_column_shard(shard, schema, fn, batch_size, progress)
    except BaseException as error:
        close_read_source(shard, error)
        if isinstance(error, Exception):
            raise _ShardError(progress.phase, error) from error
        raise
    try:
        shard.close()
    except Exception as error:
        raise _ShardError("close", error) from error
    return result
