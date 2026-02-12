#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import logging
import os
from typing import Optional

from pypaimon.common.file_io import FileIO
from pypaimon.common.json_util import JSON
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.tag.tag import Tag

logger = logging.getLogger(__name__)

TAG_PREFIX = "tag-"


class TagManager:
    """
    Manager for Tag files.
    
    This class manages tag files stored in the table's tag directory.
    Tags are essentially named snapshots that can be used for time travel queries.
    """

    def __init__(self, file_io: FileIO, table_path: str, branch: str = "main"):
        """
        Initialize TagManager.
        
        Args:
            file_io: FileIO instance for file operations
            table_path: Path to the table root directory
            branch: Branch name, defaults to "main"
        """
        self.file_io = file_io
        self.table_path = table_path.rstrip('/')
        self.branch = self._normalize_branch(branch)

    @staticmethod
    def _normalize_branch(branch: str) -> str:
        """Normalize branch name."""
        if not branch or branch == "main":
            return "main"
        return branch

    def _branch_path(self) -> str:
        """Get the branch path."""
        if self.branch == "main":
            return self.table_path
        return f"{self.table_path}/branch/branch-{self.branch}"

    def tag_directory(self) -> str:
        """Return the root directory of tags."""
        return f"{self._branch_path()}/tag"

    def tag_path(self, tag_name: str) -> str:
        """Return the path of a tag file."""
        return f"{self.tag_directory()}/{TAG_PREFIX}{tag_name}"

    def tag_exists(self, tag_name: str) -> bool:
        """Check if a tag exists."""
        path = self.tag_path(tag_name)
        return self.file_io.exists(path)

    def get(self, tag_name: str) -> Optional[Tag]:
        """
        Return the tag or None if the tag file not found.
        
        Args:
            tag_name: Name of the tag
            
        Returns:
            Tag instance or None if not found
        """
        if not tag_name or tag_name.isspace():
            raise ValueError("Tag name shouldn't be blank.")

        path = self.tag_path(tag_name)
        if not self.file_io.exists(path):
            return None

        content = self.file_io.read_file_utf8(path)
        return JSON.from_json(content, Tag)

    def get_or_throw(self, tag_name: str) -> Tag:
        """
        Return the tag or throw exception indicating the tag not found.
        
        Args:
            tag_name: Name of the tag
            
        Returns:
            Tag instance
            
        Raises:
            ValueError: If tag doesn't exist
        """
        tag = self.get(tag_name)
        if tag is None:
            raise ValueError(f"Tag '{tag_name}' doesn't exist.")
        return tag

    def create_tag(
            self,
            snapshot: Snapshot,
            tag_name: str,
            ignore_if_exists: bool = False
    ) -> None:
        """
        Create a tag from given snapshot and save it in the storage.
        
        Args:
            snapshot: The snapshot to tag
            tag_name: Name for the tag
            ignore_if_exists: If True, don't raise error if tag already exists
            
        Raises:
            ValueError: If tag_name is blank or tag already exists (when ignore_if_exists=False)
        """
        if not tag_name or tag_name.isspace():
            raise ValueError("Tag name shouldn't be blank.")

        if self.tag_exists(tag_name):
            if ignore_if_exists:
                return
            raise ValueError(f"Tag '{tag_name}' already exists.")

        self._create_or_replace_tag(snapshot, tag_name)

    def list_tag(self):
        """List all tags."""
        result = []
        for tag_file in self.file_io.list_status(self.tag_directory()):
            tag_file_name = None
            if hasattr(tag_file, 'base_name'):
                tag_file_name = tag_file.base_name
            else:
                try:
                    tag_file_name = os.path.basename(tag_file)
                except TypeError:
                    tag_file_name = None
            if tag_file_name is not None:
                _, tag = tag_file_name.split("-", 1)
                result.append(tag)
        return result

    def _create_or_replace_tag(
            self,
            snapshot: Snapshot,
            tag_name: str
    ) -> None:
        """
        Internal method to create or replace a tag.
        """
        tag_path = self.tag_path(tag_name)

        # Ensure tag directory exists
        tag_dir = self.tag_directory()
        if not self.file_io.exists(tag_dir):
            self.file_io.mkdirs(tag_dir)

        content = JSON.to_json(snapshot)

        self.file_io.overwrite_file_utf8(tag_path, content)

    def delete_tag(self, tag_name: str) -> bool:
        """
        Delete a tag.
        
        Args:
            tag_name: Name of the tag to delete
            
        Returns:
            True if tag was deleted, False if tag didn't exist
        """
        if not tag_name or tag_name.isspace():
            raise ValueError("Tag name shouldn't be blank.")

        tag = self.get(tag_name)
        if tag is None:
            logger.warning(f"Tag '{tag_name}' doesn't exist.")
            return False

        path = self.tag_path(tag_name)
        self.file_io.delete_quietly(path)
        return True
