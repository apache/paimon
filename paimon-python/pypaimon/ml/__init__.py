#
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
#

"""
PyPaimon ML/AI Integration Module

This module provides seamless integration with PyTorch and TensorFlow
for machine learning workloads on Paimon tables.

Example:
    PyTorch usage::

        from pypaimon import CatalogFactory
        from pypaimon.ml.pytorch import PaimonIterableDataset
        from torch.utils.data import DataLoader

        catalog = CatalogFactory.create({'warehouse': '/path/to/warehouse'})
        table = catalog.get_table('default.training_data')
        read_builder = table.new_read_builder()

        dataset = PaimonIterableDataset(read_builder=read_builder)
        dataloader = DataLoader(dataset, batch_size=32, num_workers=4)

    TensorFlow usage::

        from pypaimon import CatalogFactory
        from pypaimon.ml.tensorflow import PaimonTensorFlowDataset
        import tensorflow as tf

        catalog = CatalogFactory.create({'warehouse': '/path/to/warehouse'})
        table = catalog.get_table('default.training_data')
        read_builder = table.new_read_builder()

        tf_dataset = PaimonTensorFlowDataset.from_paimon(
            table=table,
            read_builder=read_builder
        )
        tf_dataset = tf_dataset.batch(32).prefetch(tf.data.AUTOTUNE)
"""

__all__ = ['pytorch', 'tensorflow']
