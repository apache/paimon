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

"""Automatic type validation and conversion for Lance format."""

import logging
from typing import Optional, Dict, List, Any, Tuple, Type
from enum import Enum

logger = logging.getLogger(__name__)


class DataType(Enum):
    """Supported data types for Lance indexes."""
    
    # Numeric types
    INT8 = "int8"
    INT16 = "int16"
    INT32 = "int32"
    INT64 = "int64"
    UINT8 = "uint8"
    UINT16 = "uint16"
    UINT32 = "uint32"
    UINT64 = "uint64"
    FLOAT32 = "float32"
    FLOAT64 = "float64"
    
    # String/Binary types
    STRING = "string"
    BINARY = "binary"
    
    # Temporal types
    DATE = "date"
    TIMESTAMP = "timestamp"
    TIME = "time"
    
    # Special types
    BOOLEAN = "bool"
    VECTOR = "vector"  # Special type for vector embeddings


class IndexTypeCompatibility(Enum):
    """Compatibility of index types with data types."""
    
    # Index type: (compatible_dtypes)
    BTREE = (
        DataType.INT8, DataType.INT16, DataType.INT32, DataType.INT64,
        DataType.UINT8, DataType.UINT16, DataType.UINT32, DataType.UINT64,
        DataType.FLOAT32, DataType.FLOAT64,
        DataType.STRING, DataType.DATE, DataType.TIMESTAMP, DataType.TIME
    )
    
    BITMAP = (
        DataType.INT8, DataType.INT16, DataType.INT32, DataType.INT64,
        DataType.UINT8, DataType.UINT16, DataType.UINT32, DataType.UINT64,
        DataType.STRING, DataType.BOOLEAN, DataType.DATE
    )
    
    IVF_PQ = (DataType.VECTOR, DataType.FLOAT32, DataType.FLOAT64)
    
    HNSW = (DataType.VECTOR, DataType.FLOAT32, DataType.FLOAT64)


class TypeValidator:
    """
    Validates and auto-detects data types for Lance indexes.
    
    Features:
    - Automatic data type detection from samples
    - Type compatibility checking
    - Safe type conversion
    - Validation error reporting
    """
    
    def __init__(self):
        """Initialize type validator."""
        self._type_cache: Dict[str, DataType] = {}
    
    def detect_type(self, data: Any, column_name: str = "") -> DataType:
        """
        Detect data type from sample values.
        
        Args:
            data: Sample data (value or list of values)
            column_name: Optional column name for caching
            
        Returns:
            Detected DataType
        """
        # Check cache first
        if column_name and column_name in self._type_cache:
            return self._type_cache[column_name]
        
        # Detect type from data
        detected_type = self._infer_type(data)
        
        # Cache result
        if column_name:
            self._type_cache[column_name] = detected_type
        
        logger.debug(f"Detected type for {column_name}: {detected_type}")
        return detected_type
    
    def validate_index_compatibility(self, 
                                    index_type: str,
                                    data_type: DataType) -> Tuple[bool, Optional[str]]:
        """
        Validate if data type is compatible with index type.
        
        Args:
            index_type: Type of index (ivf_pq, hnsw, btree, bitmap)
            data_type: Data type to validate
            
        Returns:
            Tuple of (is_compatible, error_message)
        """
        index_type = index_type.lower()
        
        try:
            # Get compatible types for this index
            if index_type == 'ivf_pq':
                compatible = IndexTypeCompatibility.IVF_PQ.value
            elif index_type == 'hnsw':
                compatible = IndexTypeCompatibility.HNSW.value
            elif index_type == 'btree':
                compatible = IndexTypeCompatibility.BTREE.value
            elif index_type == 'bitmap':
                compatible = IndexTypeCompatibility.BITMAP.value
            else:
                return False, f"Unknown index type: {index_type}"
            
            # Check compatibility
            is_compatible = data_type in compatible
            
            if is_compatible:
                return True, None
            else:
                compatible_names = [t.value for t in compatible]
                error_msg = (
                    f"Data type '{data_type.value}' is not compatible with "
                    f"'{index_type}' index. Compatible types: {compatible_names}"
                )
                return False, error_msg
            
        except Exception as e:
            return False, f"Validation error: {str(e)}"
    
    def validate_batch(self, batch: Any, expected_type: Optional[DataType] = None) -> Dict[str, Any]:
        """
        Validate a batch of data for type consistency.
        
        Args:
            batch: PyArrow RecordBatch or similar
            expected_type: Expected data type (if known)
            
        Returns:
            Validation result dictionary
        """
        result = {
            'is_valid': True,
            'num_rows': 0,
            'num_nulls': 0,
            'detected_type': None,
            'type_errors': [],
            'inconsistencies': []
        }
        
        try:
            # Get batch size
            num_rows = batch.num_rows if hasattr(batch, 'num_rows') else len(batch)
            result['num_rows'] = num_rows
            
            # Detect type from batch
            detected_type = self.detect_type(batch)
            result['detected_type'] = detected_type
            
            # Check consistency with expected type
            if expected_type and detected_type != expected_type:
                result['is_valid'] = False
                result['inconsistencies'].append(
                    f"Type mismatch: expected {expected_type.value}, got {detected_type.value}"
                )
            
            # Check for NULL values
            null_count = self._count_nulls(batch)
            result['num_nulls'] = null_count
            
            if null_count > 0:
                null_ratio = null_count / num_rows if num_rows > 0 else 0
                logger.warning(f"Found {null_count} NULL values ({null_ratio:.1%})")
            
            return result
            
        except Exception as e:
            result['is_valid'] = False
            result['type_errors'].append(str(e))
            return result
    
    def validate_schema(self, schema: Dict[str, str], 
                       index_definitions: Dict[str, str]) -> Dict[str, Any]:
        """
        Validate schema compatibility with index definitions.
        
        Args:
            schema: Dictionary mapping column names to data types
            index_definitions: Dictionary mapping column names to index types
            
        Returns:
            Validation report
        """
        report = {
            'is_valid': True,
            'total_columns': len(schema),
            'indexed_columns': len(index_definitions),
            'compatible': [],
            'incompatible': [],
            'warnings': []
        }
        
        for column, index_type in index_definitions.items():
            if column not in schema:
                report['is_valid'] = False
                report['incompatible'].append({
                    'column': column,
                    'index': index_type,
                    'error': f"Column '{column}' not found in schema"
                })
                continue
            
            # Parse data type string to DataType
            dtype_str = schema[column].lower()
            try:
                data_type = self._parse_dtype_string(dtype_str)
            except ValueError as e:
                report['incompatible'].append({
                    'column': column,
                    'index': index_type,
                    'error': f"Unknown data type: {dtype_str}"
                })
                continue
            
            # Check compatibility
            is_compat, error = self.validate_index_compatibility(index_type, data_type)
            
            if is_compat:
                report['compatible'].append({
                    'column': column,
                    'index': index_type,
                    'data_type': data_type.value
                })
            else:
                report['is_valid'] = False
                report['incompatible'].append({
                    'column': column,
                    'index': index_type,
                    'error': error
                })
        
        return report
    
    def recommend_index_type(self, data_type: DataType) -> Optional[str]:
        """
        Recommend index type for a data type.
        
        Args:
            data_type: Data type
            
        Returns:
            Recommended index type, or None if no suitable index
        """
        if data_type == DataType.VECTOR:
            return 'ivf_pq'  # Default to IVF_PQ for vectors
        elif data_type in (DataType.FLOAT32, DataType.FLOAT64):
            return 'ivf_pq'  # Assume float columns are vectors
        elif data_type in (DataType.INT8, DataType.INT16, DataType.INT32, 
                          DataType.INT64, DataType.UINT8, DataType.UINT16,
                          DataType.UINT32, DataType.UINT64, DataType.FLOAT32,
                          DataType.FLOAT64, DataType.DATE, DataType.TIMESTAMP):
            return 'btree'  # Range queries
        elif data_type in (DataType.STRING, DataType.BOOLEAN):
            return 'bitmap'  # Low cardinality
        else:
            return None
    
    def safe_convert(self, value: Any, target_type: DataType) -> Any:
        """
        Safely convert a value to target type.
        
        Args:
            value: Value to convert
            target_type: Target data type
            
        Returns:
            Converted value, or original if conversion not possible
        """
        if value is None:
            return None
        
        try:
            if target_type == DataType.INT32:
                return int(value)
            elif target_type == DataType.INT64:
                return int(value)
            elif target_type == DataType.FLOAT32:
                return float(value)
            elif target_type == DataType.FLOAT64:
                return float(value)
            elif target_type == DataType.STRING:
                return str(value)
            elif target_type == DataType.BOOLEAN:
                if isinstance(value, bool):
                    return value
                return str(value).lower() in ('true', '1', 'yes')
            else:
                return value
        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to convert {value} to {target_type.value}: {e}")
            return value
    
    @staticmethod
    def _infer_type(data: Any) -> DataType:
        """Infer data type from sample."""
        if data is None:
            return DataType.STRING
        
        if isinstance(data, (list, tuple)):
            if len(data) == 0:
                return DataType.STRING
            # Use first non-null element
            for item in data:
                if item is not None:
                    return TypeValidator._infer_type(item)
            return DataType.STRING
        
        if isinstance(data, bool):
            return DataType.BOOLEAN
        elif isinstance(data, int):
            # Default to INT32 for most cases, use INT64 only for large values
            if -2147483648 <= data <= 2147483647:
                return DataType.INT32
            else:
                return DataType.INT64
        elif isinstance(data, float):
            return DataType.FLOAT64
        elif isinstance(data, str):
            return DataType.STRING
        elif isinstance(data, bytes):
            return DataType.BINARY
        else:
            # Try to detect if it's a vector
            try:
                if hasattr(data, '__iter__') and hasattr(data, '__len__'):
                    if len(data) > 0:
                        # Check if all elements are numeric
                        first = next(iter(data))
                        if isinstance(first, (int, float)):
                            return DataType.VECTOR
            except (TypeError, StopIteration):
                pass
            
            return DataType.STRING
    
    @staticmethod
    def _parse_dtype_string(dtype_str: str) -> DataType:
        """Parse data type from string."""
        dtype_str = dtype_str.lower().strip()
        
        # Try exact match first
        for dtype in DataType:
            if dtype.value == dtype_str:
                return dtype
        
        # Try partial match
        if 'int' in dtype_str:
            if '8' in dtype_str:
                return DataType.INT8 if 'u' not in dtype_str else DataType.UINT8
            elif '16' in dtype_str:
                return DataType.INT16 if 'u' not in dtype_str else DataType.UINT16
            elif '32' in dtype_str:
                return DataType.INT32 if 'u' not in dtype_str else DataType.UINT32
            elif '64' in dtype_str:
                return DataType.INT64 if 'u' not in dtype_str else DataType.UINT64
            else:
                return DataType.INT64
        elif 'float' in dtype_str or 'double' in dtype_str:
            if '32' in dtype_str:
                return DataType.FLOAT32
            else:
                return DataType.FLOAT64
        elif 'string' in dtype_str or 'varchar' in dtype_str or 'text' in dtype_str:
            return DataType.STRING
        elif 'bool' in dtype_str:
            return DataType.BOOLEAN
        elif 'date' in dtype_str:
            return DataType.DATE
        elif 'timestamp' in dtype_str:
            return DataType.TIMESTAMP
        elif 'vector' in dtype_str or 'embedding' in dtype_str:
            return DataType.VECTOR
        
        raise ValueError(f"Unknown data type: {dtype_str}")
    
    @staticmethod
    def _count_nulls(batch: Any) -> int:
        """Count NULL values in batch."""
        try:
            if hasattr(batch, 'null_count'):
                return batch.null_count
            elif isinstance(batch, (list, tuple)):
                return sum(1 for x in batch if x is None)
            else:
                return 0
        except Exception:
            return 0


class SchemaBuilder:
    """
    Helper class for building and validating schemas.
    """
    
    def __init__(self):
        """Initialize schema builder."""
        self.validator = TypeValidator()
        self.columns: Dict[str, DataType] = {}
    
    def add_column(self, name: str, dtype: DataType) -> "SchemaBuilder":
        """
        Add a column to schema.
        
        Args:
            name: Column name
            dtype: Data type
            
        Returns:
            Self for chaining
        """
        self.columns[name] = dtype
        return self
    
    def infer_from_sample(self, sample_data: Dict[str, Any]) -> "SchemaBuilder":
        """
        Infer schema from sample data.
        
        Args:
            sample_data: Dictionary mapping column names to sample values
            
        Returns:
            Self for chaining
        """
        for col_name, col_data in sample_data.items():
            dtype = self.validator.detect_type(col_data, col_name)
            self.columns[col_name] = dtype
        
        return self
    
    def validate(self) -> Tuple[bool, List[str]]:
        """
        Validate schema consistency.
        
        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors = []
        
        if not self.columns:
            errors.append("Schema has no columns")
        
        # Check for duplicate columns (shouldn't happen in dict, but be safe)
        if len(self.columns) != len(set(self.columns.keys())):
            errors.append("Duplicate column names detected")
        
        return len(errors) == 0, errors
    
    def build(self) -> Dict[str, DataType]:
        """Build and return the schema."""
        is_valid, errors = self.validate()
        if not is_valid:
            raise ValueError(f"Invalid schema: {errors}")
        
        return self.columns.copy()
