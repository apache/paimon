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

import json
import os
import sys
from types import SimpleNamespace

import torch
from torch.utils.data import DataLoader
from torch.nn.parallel import DistributedDataParallel

from pypaimon.read.datasource.torch_dataset import TorchIterDataset


class _OffsetRow:
    def __init__(self, values):
        self._values = values

    def get_field(self, index):
        return self._values[index]


class _TableRead:
    limit = None
    read_type = [
        SimpleNamespace(name="split_id"),
        SimpleNamespace(name="rank"),
        SimpleNamespace(name="worker"),
    ]

    def __init__(self):
        self.rank = None

    def to_iterator(self, splits):
        worker_info = torch.utils.data.get_worker_info()
        worker_id = worker_info.id if worker_info is not None else 0
        for split_id in splits:
            yield _OffsetRow([split_id, self.rank, worker_id])


def main():
    output_dir = sys.argv[1]
    rank_env = os.environ.pop("RANK")
    world_size_env = os.environ.pop("WORLD_SIZE")
    table_read = _TableRead()
    dataset = TorchIterDataset(
        table_read,
        list(range(11)),
        auto_detect_rank=True,
    )
    os.environ["RANK"] = rank_env
    os.environ["WORLD_SIZE"] = world_size_env
    torch.distributed.init_process_group("gloo")
    rank = torch.distributed.get_rank()
    try:
        table_read.rank = rank
        os.environ.pop("RANK")
        os.environ.pop("WORLD_SIZE")
        loader = DataLoader(
            dataset,
            batch_size=None,
            num_workers=2,
            multiprocessing_context="spawn",
        )
        model = DistributedDataParallel(torch.nn.Linear(1, 1))
        optimizer = torch.optim.SGD(model.parameters(), lr=0.01)
        rows = []
        with model.join():
            for row in loader:
                rows.append(row)
                value = torch.tensor([[float(row["split_id"])]])
                model(value).sum().backward()
                optimizer.step()
                optimizer.zero_grad()
        with open(
            os.path.join(output_dir, "rank-%d.json" % rank),
            "w",
            encoding="utf-8",
        ) as result_file:
            json.dump(rows, result_file)
        torch.distributed.barrier()
    finally:
        torch.distributed.destroy_process_group()


if __name__ == "__main__":
    main()
