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

import logging
from abc import ABC, abstractmethod
from typing import List, Any

logger = logging.getLogger(__name__)


class CompactStrategy(ABC):
    """Base class for compaction strategies."""

    @abstractmethod
    def select_files(self, files: List[Any]) -> List[Any]:
        """
        Select files for compaction based on this strategy.

        Args:
            files: List of DataFileMeta objects

        Returns:
            List of selected files to compact
        """
        pass


class FullCompactStrategy(CompactStrategy):
    """Strategy that selects all files for compaction."""

    def select_files(self, files: List[Any]) -> List[Any]:
        """
        Select all files for compaction.

        Args:
            files: List of DataFileMeta objects

        Returns:
            All files
        """
        logger.debug("Full compact strategy: selecting all %d files", len(files))
        return files


class MinorCompactStrategy(CompactStrategy):
    """
    Strategy that selects files based on size and age.

    Prioritizes smaller files for compaction to consolidate small files
    into larger, more efficient files.
    """

    def __init__(self, target_file_size: int = 256 * 1024 * 1024):
        """
        Initialize the minor compact strategy.

        Args:
            target_file_size: Target size for compacted files (default 256MB)
        """
        self.target_file_size = target_file_size

    def select_files(self, files: List[Any]) -> List[Any]:
        """
        Select files for minor compaction.

        Selects files that are smaller than the target size.

        Args:
            files: List of DataFileMeta objects

        Returns:
            List of selected files smaller than target size
        """
        if not files:
            return []

        # Filter files that are smaller than target size
        selected = []
        for file_meta in files:
            try:
                if hasattr(file_meta, 'file_size'):
                    file_size = file_meta.file_size()
                    if file_size < self.target_file_size:
                        selected.append(file_meta)
                else:
                    # If we can't determine size, include the file
                    selected.append(file_meta)
            except Exception as e:
                logger.warning(
                    "Error checking file size: %s",
                    str(e)
                )
                selected.append(file_meta)

        logger.debug(
            "Minor compact strategy: selected %d/%d files (target size: %d bytes)",
            len(selected),
            len(files),
            self.target_file_size
        )

        return selected


class CompactionStrategyFactory:
    """Factory for creating compaction strategies."""

    @staticmethod
    def create_strategy(
        strategy_name: str,
        target_file_size: int = 256 * 1024 * 1024
    ) -> CompactStrategy:
        """
        Create a compaction strategy by name.

        Args:
            strategy_name: Strategy name ('full', 'minor', etc.)
            target_file_size: Target file size for minor strategy

        Returns:
            CompactStrategy instance

        Raises:
            ValueError: If strategy name is unknown
        """
        strategy_name = strategy_name.lower().strip()

        if strategy_name in ('full', 'fullcompact'):
            logger.debug("Creating FullCompactStrategy")
            return FullCompactStrategy()
        elif strategy_name in ('minor', 'minorcompact'):
            logger.debug("Creating MinorCompactStrategy with target size %d", target_file_size)
            return MinorCompactStrategy(target_file_size)
        else:
            raise ValueError(f"Unknown compaction strategy: {strategy_name}")
