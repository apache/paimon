/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.tag;

import org.apache.paimon.Snapshot;

import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.time.LocalDateTime;

/** Test for {@link Tag}. */
public class TagTest {

    private final Snapshot snapshot =
            new Snapshot(
                    0,
                    0L,
                    null,
                    null,
                    null,
                    null,
                    null,
                    0L,
                    Snapshot.CommitKind.APPEND,
                    1000,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null);

    @Test
    public void testFromJson() {
        Tag tag = Tag.fromJson(snapshot.toJson());
        Assert.assertEquals(
                "{\n"
                        + "  \"version\" : 3,\n"
                        + "  \"id\" : 0,\n"
                        + "  \"schemaId\" : 0,\n"
                        + "  \"baseManifestList\" : null,\n"
                        + "  \"deltaManifestList\" : null,\n"
                        + "  \"changelogManifestList\" : null,\n"
                        + "  \"commitUser\" : null,\n"
                        + "  \"commitIdentifier\" : 0,\n"
                        + "  \"commitKind\" : \"APPEND\",\n"
                        + "  \"timeMillis\" : 1000,\n"
                        + "  \"totalRecordCount\" : null,\n"
                        + "  \"deltaRecordCount\" : null\n"
                        + "}",
                tag.toJson());
    }

    @Test
    public void testFromSnapshotAndTagTtl() {
        Tag tag =
                Tag.fromSnapshotAndTagTtl(
                        snapshot,
                        Duration.ofSeconds(5),
                        LocalDateTime.of(1969, 1, 1, 0, 0, 0, 123456789));
        String tagJson = tag.toJson();
        Assert.assertEquals(
                "{\n"
                        + "  \"version\" : 3,\n"
                        + "  \"id\" : 0,\n"
                        + "  \"schemaId\" : 0,\n"
                        + "  \"baseManifestList\" : null,\n"
                        + "  \"deltaManifestList\" : null,\n"
                        + "  \"changelogManifestList\" : null,\n"
                        + "  \"commitUser\" : null,\n"
                        + "  \"commitIdentifier\" : 0,\n"
                        + "  \"commitKind\" : \"APPEND\",\n"
                        + "  \"timeMillis\" : 1000,\n"
                        + "  \"totalRecordCount\" : null,\n"
                        + "  \"deltaRecordCount\" : null,\n"
                        + "  \"tagCreateTime\" : [ 1969, 1, 1, 0, 0, 0, 123456789 ],\n"
                        + "  \"tagTimeRetained\" : 5.000000000\n"
                        + "}",
                tagJson);

        Tag newTag = Tag.fromJson(tagJson);
        Assert.assertEquals(tag, newTag);
    }
}
