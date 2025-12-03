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

"""Utility functions for Lance format support."""

from typing import Dict, Optional, Any, List
from pathlib import Path
from pypaimon.common.file_io import FileIO


class LanceUtils:
    """Utility class for Lance format operations."""

    @staticmethod
    def convert_to_lance_storage_options(file_io: FileIO, file_path: str) -> Dict[str, str]:
        """
        Convert Paimon FileIO configuration to Lance storage options.
        
        Args:
            file_io: Paimon FileIO instance
            file_path: File path to access
            
        Returns:
            Dictionary of Lance storage options
        """
        storage_options: Dict[str, str] = {}
        
        # Get the URI scheme
        try:
            uri_str = str(file_path)
            
            # For local filesystem paths
            if uri_str.startswith('/') or ':\\' in uri_str:  # Unix or Windows path
                # Local filesystem - no special options needed
                return storage_options
            
            # Parse URI scheme
            if '://' in uri_str:
                scheme = uri_str.split('://')[0].lower()
                
                # For S3 and OSS, Lance can handle them natively with minimum config
                # Most cloud storage credentials are typically set via environment variables
                # or via the FileIO's internal configuration
                if scheme in ('oss', 's3', 's3a'):
                    # Lance can read S3-compatible URIs directly
                    pass
            
        except Exception as e:
            # If anything fails, return empty options and let Lance handle it
            import logging
            logging.warning(f"Failed to extract storage options: {e}")
            return {}
        
        return storage_options

    @staticmethod
    def convert_uri_to_local_path(file_io: FileIO, file_path: str) -> str:
        """
        Convert file path URI to local filesystem path suitable for Lance.
        
        Args:
            file_io: Paimon FileIO instance
            file_path: File path URI
            
        Returns:
            Local filesystem path
        """
        uri_str = str(file_path)
        
        # For OSS URIs, convert to S3-compatible format
        if uri_str.startswith('oss://'):
            # Convert oss://bucket/path to s3://bucket/path
            return uri_str.replace('oss://', 's3://', 1)
        
        # For local paths or regular S3 paths, return as-is
        return uri_str

    @staticmethod
    def convert_row_ranges_to_list(row_ids: Optional[Any]) -> Optional[List[tuple]]:
        """
        Convert RoaringBitmap32 or similar row ID selection to list of (start, end) ranges.
        
        Args:
            row_ids: RoaringBitmap32 or row ID selection object
            
        Returns:
            List of (start, end) tuples or None
        """
        if row_ids is None:
            return None
        
        try:
            # Try to convert RoaringBitmap32
            if hasattr(row_ids, '__iter__') and not isinstance(row_ids, str):
                # If it's iterable (but not string), convert to list of ranges
                try:
                    # Cast to iterable and convert to list
                    row_id_list = [int(i) for i in row_ids]  # type: ignore
                    sorted_ids = sorted(row_id_list)
                except (TypeError, ValueError):
                    return None
                    
                if not sorted_ids:
                    return None
                
                ranges: List[tuple] = []
                start = sorted_ids[0]
                end = start + 1
                
                for row_id in sorted_ids[1:]:
                    if row_id == end:
                        end += 1
                    else:
                        ranges.append((start, end))
                        start = row_id
                        end = start + 1
                
                ranges.append((start, end))
                return ranges if ranges else None
            
        except Exception as e:
            import logging
            logging.warning(f"Failed to convert row ranges: {e}")
            return None
        
        return None
