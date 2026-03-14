---
title: "PyTorch"
weight: 4
type: docs
aliases:
  - /pypaimon/pytorch.html
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# PyTorch

## Read

This requires `torch` to be installed.

You can read all the data into a `torch.utils.data.Dataset` or `torch.utils.data.IterableDataset`:

```python
from torch.utils.data import DataLoader

table_read = read_builder.new_read()
dataset = table_read.to_torch(splits, streaming=True, prefetch_concurrency=2)
dataloader = DataLoader(
    dataset,
    batch_size=2,
    num_workers=2,  # Concurrency to read data
    shuffle=False
)

# Collect all data from dataloader
for batch_idx, batch_data in enumerate(dataloader):
    print(batch_data)

# output:
#   {'user_id': tensor([1, 2]), 'behavior': ['a', 'b']}
#   {'user_id': tensor([3, 4]), 'behavior': ['c', 'd']}
#   {'user_id': tensor([5, 6]), 'behavior': ['e', 'f']}
#   {'user_id': tensor([7, 8]), 'behavior': ['g', 'h']}
```

When the `streaming` parameter is true, it will iteratively read;
when it is false, it will read the full amount of data into memory.

**`prefetch_concurrency`** (default: 1): When streaming is true, number of threads used for parallel prefetch within each DataLoader worker. Set to a value greater than 1 to partition splits across threads and increase read throughput. Has no effect when streaming is false.
