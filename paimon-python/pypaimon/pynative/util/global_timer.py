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

import time
import functools
from threading import local
from contextlib import contextmanager

_thread_local = local()


def _get_thread_storage():
    if not hasattr(_thread_local, 'storage'):
        _thread_local.storage = {
            'is_enabled': True,
            'records': [],
            'scope_stack': []
        }
    return _thread_local.storage


class GlobalTimer:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    @property
    def _storage(self):
        return _get_thread_storage()

    def enable(self):
        self._storage['is_enabled'] = True

    def disable(self):
        self._storage['is_enabled'] = False

    def is_enabled(self):
        return self._storage['is_enabled']

    def reset(self):
        self._storage['records'] = []
        self._storage['scope_stack'] = []
        self.mark("start")

    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._initialized = True
            self.reset()

    def mark(self, name: str):
        if not self.is_enabled():
            return

        depth = len(self._storage['scope_stack'])
        self._storage['records'].append({
            'name': name,
            'type': 'mark',
            'time': time.perf_counter(),
            'depth': depth
        })

    @contextmanager
    def scope(self, name: str):
        if not self.is_enabled():
            yield
            return

        depth = len(self._storage['scope_stack'])
        start_time = time.perf_counter()

        self._storage['scope_stack'].append(name)
        self._storage['records'].append({
            'name': name,
            'type': 'scope_start',
            'time': start_time,
            'depth': depth
        })

        try:
            yield
        finally:
            end_time = time.perf_counter()
            self._storage['records'].append({
                'name': name,
                'type': 'scope_end',
                'time': end_time,
                'duration': end_time - start_time,
                'depth': depth
            })
            self._storage['scope_stack'].pop()

    def timed(self, name: str = None):

        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                scope_name = name if name is not None else func.__name__
                with self.scope(scope_name):
                    return func(*args, **kwargs)

            return wrapper

        return decorator

    def report(self, sort_by='time'):
        if not self.is_enabled():
            print("GlobalTimer is disabled.")
            return

        records = self._storage['records']
        if not records:
            print("No records to report.")
            return

        print("\n--- Global Timer Report ---")

        print("\n[Timeline View]")
        start_time = records[0]['time']
        for record in records:
            indent = "  " * record['depth']
            elapsed = record['time'] - start_time
            if record['type'] == 'mark':
                print(f"{indent}{record['name']:<20} | at {elapsed:8.4f}s")
            elif record['type'] == 'scope_start':
                print(f"{indent}Scope Start: {record['name']:<20} | at {elapsed:8.4f}s")
            elif record['type'] == 'scope_end':
                print(
                    f"{indent}Scope End:   {record['name']:<20} | at {elapsed:8.4f}s (took {record['duration']:.4f}s)")

        print("\n[Top Scopes by Duration]")
        scopes = [r for r in records if r['type'] == 'scope_end']
        sorted_scopes = sorted(scopes, key=lambda x: x['duration'], reverse=True)

        for scope in sorted_scopes[:10]:  # 只显示耗时最长的前10个
            print(f"- {scope['name']:<25}: {scope['duration']:.4f}s")

        print("\n--- End of Report ---\n")


profiler = GlobalTimer()
