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

"""Lance format support modules including vector indexing, scalar indexing, and predicate optimization."""

try:
    from pypaimon.read.reader.lance.vector_index import VectorIndexBuilder
    from pypaimon.read.reader.lance.scalar_index import ScalarIndexBuilder, BitmapIndexHandler, BTreeIndexHandler
    from pypaimon.read.reader.lance.predicate_pushdown import PredicateOptimizer, PredicateExpression, PredicateOperator
    from pypaimon.read.reader.lance.lance_utils import LanceUtils
    from pypaimon.read.reader.lance.lance_native_reader import LanceNativeReader
    
    __all__ = [
        'VectorIndexBuilder',
        'ScalarIndexBuilder',
        'BitmapIndexHandler',
        'BTreeIndexHandler',
        'PredicateOptimizer',
        'PredicateExpression',
        'PredicateOperator',
        'LanceUtils',
        'LanceNativeReader',
    ]
except ImportError:
    # Lance library not available
    __all__ = []
