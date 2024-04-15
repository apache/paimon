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
import java.util.ArrayList;
import java.util.List;

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

    @Test
    public void testTagComparator() {
        Tag tag1 =
                new Tag(
                        3,
                        2L,
                        0,
                        null,
                        null,
                        null,
                        null,
                        null,
                        8,
                        Snapshot.CommitKind.APPEND,
                        1000,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        LocalDateTime.now(),
                        Duration.ofSeconds(10));

        Tag tag2 =
                new Tag(
                        3,
                        1L,
                        0,
                        null,
                        null,
                        null,
                        null,
                        null,
                        8,
                        Snapshot.CommitKind.APPEND,
                        1000,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        LocalDateTime.now(),
                        Duration.ofSeconds(10));

        Tag tag3 =
                new Tag(
                        3,
                        0L,
                        0,
                        null,
                        null,
                        null,
                        null,
                        null,
                        8,
                        Snapshot.CommitKind.APPEND,
                        1000,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        LocalDateTime.now(),
                        Duration.ofSeconds(10));
        List<Tag> tags = new ArrayList<>();
        tags.add(tag1);
        tags.add(tag2);
        tags.add(tag3);
        tags.sort(Tag.TAG_COMPARATOR);
        Assert.assertEquals(0, tags.get(0).id());
        Assert.assertEquals(1, tags.get(1).id());
        Assert.assertEquals(2, tags.get(2).id());
    }
}
