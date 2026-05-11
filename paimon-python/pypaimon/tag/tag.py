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


from pypaimon.snapshot.snapshot import Snapshot


class Tag(Snapshot):

    def trim_to_snapshot(self) -> Snapshot:
        """Convert this Tag to a Snapshot"""
        return Snapshot(
            version=self.version,
            id=self.id,
            schema_id=self.schema_id,
            base_manifest_list=self.base_manifest_list,
            delta_manifest_list=self.delta_manifest_list,
            total_record_count=self.total_record_count,
            delta_record_count=self.delta_record_count,
            commit_user=self.commit_user,
            commit_identifier=self.commit_identifier,
            commit_kind=self.commit_kind,
            time_millis=self.time_millis,
            changelog_manifest_list=self.changelog_manifest_list,
            index_manifest=self.index_manifest,
            changelog_record_count=self.changelog_record_count,
            watermark=self.watermark,
            statistics=self.statistics,
            next_row_id=self.next_row_id
        )
