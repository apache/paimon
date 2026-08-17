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

import importlib
import sys
import threading

if sys.version_info[:2] < (3, 7):
    # Module-level __getattr__ is unavailable before Python 3.7.
    from pypaimon.globalindex.global_index_result import GlobalIndexResult
    from pypaimon.globalindex.global_index_reader import GlobalIndexReader, FieldRef
    from pypaimon.globalindex.vector_search import VectorSearch
    from pypaimon.globalindex.full_text_search import FullTextSearch
    from pypaimon.globalindex.vector_search_result import (
        ScoredGlobalIndexResult,
        DictBasedScoredIndexResult,
        ScoreGetter,
    )
    from pypaimon.globalindex.global_index_meta import GlobalIndexMeta, GlobalIndexIOMeta
    from pypaimon.globalindex.global_index_evaluator import GlobalIndexEvaluator
    from pypaimon.globalindex.data_evolution_global_index_scanner import (
        DataEvolutionGlobalIndexScanner,
    )
    from pypaimon.globalindex.key_serializer import KeySerializer
    from pypaimon.globalindex.memory_slice_input import MemorySliceInput
    from pypaimon.globalindex.offset_global_index_reader import OffsetGlobalIndexReader
    from pypaimon.globalindex.sorted_file_global_index_reader import SortedFileGlobalIndexReader
    from pypaimon.globalindex.sorted_file_meta_selector import SortedFileMetaSelector
    from pypaimon.globalindex.sorted_index_file_meta import SortedIndexFileMeta
    from pypaimon.globalindex.create_global_index import (
        GlobalIndexBuilder,
        create_global_index,
    )
    from pypaimon.globalindex.drop_global_index import (
        GlobalIndexDropper,
        drop_global_index,
    )
    from pypaimon.utils.range import Range

__all__ = [
    'GlobalIndexResult',
    'GlobalIndexReader',
    'FieldRef',
    'VectorSearch',
    'FullTextSearch',
    'ScoredGlobalIndexResult',
    'DictBasedScoredIndexResult',
    'ScoreGetter',
    'GlobalIndexMeta',
    'GlobalIndexIOMeta',
    'GlobalIndexEvaluator',
    'DataEvolutionGlobalIndexScanner',
    'KeySerializer',
    'MemorySliceInput',
    'OffsetGlobalIndexReader',
    'SortedFileGlobalIndexReader',
    'SortedFileMetaSelector',
    'SortedIndexFileMeta',
    'GlobalIndexBuilder',
    'create_global_index',
    'GlobalIndexDropper',
    'drop_global_index',
    'Range',
]

_MODULE_BY_EXPORT = {
    'GlobalIndexResult': 'pypaimon.globalindex.global_index_result',
    'GlobalIndexReader': 'pypaimon.globalindex.global_index_reader',
    'FieldRef': 'pypaimon.globalindex.global_index_reader',
    'VectorSearch': 'pypaimon.globalindex.vector_search',
    'FullTextSearch': 'pypaimon.globalindex.full_text_search',
    'ScoredGlobalIndexResult': 'pypaimon.globalindex.vector_search_result',
    'DictBasedScoredIndexResult': 'pypaimon.globalindex.vector_search_result',
    'ScoreGetter': 'pypaimon.globalindex.vector_search_result',
    'GlobalIndexMeta': 'pypaimon.globalindex.global_index_meta',
    'GlobalIndexIOMeta': 'pypaimon.globalindex.global_index_meta',
    'GlobalIndexEvaluator': 'pypaimon.globalindex.global_index_evaluator',
    'DataEvolutionGlobalIndexScanner':
        'pypaimon.globalindex.data_evolution_global_index_scanner',
    'KeySerializer': 'pypaimon.globalindex.key_serializer',
    'MemorySliceInput': 'pypaimon.globalindex.memory_slice_input',
    'OffsetGlobalIndexReader':
        'pypaimon.globalindex.offset_global_index_reader',
    'SortedFileGlobalIndexReader':
        'pypaimon.globalindex.sorted_file_global_index_reader',
    'SortedFileMetaSelector':
        'pypaimon.globalindex.sorted_file_meta_selector',
    'SortedIndexFileMeta': 'pypaimon.globalindex.sorted_index_file_meta',
    'GlobalIndexBuilder': 'pypaimon.globalindex.create_global_index',
    'create_global_index': 'pypaimon.globalindex.create_global_index',
    'GlobalIndexDropper': 'pypaimon.globalindex.drop_global_index',
    'drop_global_index': 'pypaimon.globalindex.drop_global_index',
    'Range': 'pypaimon.utils.range',
}

# Eagerly importing the exports above builds a circular chain: submodules
# such as index_file_meta initialize this package on first import, while
# create_global_index and the scanner chain import those submodules back.
# Lazy resolution keeps this package init trivial; the lock serializes
# first-time imports racing from multiple threads.
_LAZY_IMPORT_LOCK = threading.RLock()


def __getattr__(name):
    module_name = _MODULE_BY_EXPORT.get(name)
    if module_name is None:
        raise AttributeError(
            "module 'pypaimon.globalindex' has no attribute {}".format(name))
    with _LAZY_IMPORT_LOCK:
        if name not in globals():
            module = importlib.import_module(module_name)
            globals()[name] = getattr(module, name)
        return globals()[name]
