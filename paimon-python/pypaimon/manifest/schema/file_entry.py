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

"""Entry representing a file.

Follows the design of Java's org.apache.paimon.manifest.FileEntry.
"""


class FileEntry:
    """Entry representing a file.

    The same Identifier indicates that the FileEntry refers to the same data file.
    """

    class Identifier:
        """Unique identifier for a file entry.

        Uses partition, bucket, level, fileName, extraFiles,
        embeddedIndex and externalPath to identify a file.
        """

        def __init__(self, partition, bucket, level, file_name,
                     extra_files, embedded_index, external_path):
            self.partition = partition
            self.bucket = bucket
            self.level = level
            self.file_name = file_name
            self.extra_files = extra_files
            self.embedded_index = embedded_index
            self.external_path = external_path
            self._hash = None

        def __eq__(self, other):
            if self is other:
                return True
            if other is None or not isinstance(other, FileEntry.Identifier):
                return False
            return (self.bucket == other.bucket
                    and self.level == other.level
                    and self.partition == other.partition
                    and self.file_name == other.file_name
                    and self.extra_files == other.extra_files
                    and self.embedded_index == other.embedded_index
                    and self.external_path == other.external_path)

        def __hash__(self):
            if self._hash is None:
                self._hash = hash((
                    self.partition,
                    self.bucket,
                    self.level,
                    self.file_name,
                    self.extra_files,
                    self.embedded_index,
                    self.external_path,
                ))
            return self._hash

    def identifier(self):
        """Build a unique Identifier for this file entry.

        Returns:
            An Identifier instance.
        """
        extra_files = (tuple(self.file.extra_files)
                       if self.file.extra_files else ())
        return FileEntry.Identifier(
            partition=self.partition,
            bucket=self.bucket,
            level=self.file.level,
            file_name=self.file.file_name,
            extra_files=extra_files,
            embedded_index=self.file.embedded_index,
            external_path=self.file.external_path,
        )

    @staticmethod
    def merge_entries(entries):
        """Merge file entries: ADD and DELETE of the same file cancel each other.

        - ADD: if identifier already in map, raise error; otherwise add to map.
        - DELETE: if identifier already in map, remove both (cancel);
          otherwise add to map.

        Args:
            entries: Iterable of FileEntry.

        Returns:
            List of merged FileEntry values, preserving insertion order.

        Raises:
            RuntimeError: If trying to add a file that is already in the map.
        """
        entry_map = {}

        for entry in entries:
            entry_identifier = entry.identifier()
            if entry.kind == 0:  # ADD
                if entry_identifier in entry_map:
                    raise RuntimeError(
                        "Trying to add file {} which is already added.".format(
                            entry.file.file_name))
                entry_map[entry_identifier] = entry
            elif entry.kind == 1:  # DELETE
                if entry_identifier in entry_map:
                    del entry_map[entry_identifier]
                else:
                    entry_map[entry_identifier] = entry
            else:
                raise RuntimeError(
                    "Unknown entry kind: {}".format(entry.kind))

        return list(entry_map.values())
