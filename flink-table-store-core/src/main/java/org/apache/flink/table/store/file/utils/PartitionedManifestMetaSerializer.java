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

package org.apache.flink.table.store.file.utils;

import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.data.DataFileMetaSerializer;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** JSON serializer for {@link PartitionedManifestMeta}. */
public class PartitionedManifestMetaSerializer implements JsonSerializer<PartitionedManifestMeta> {

    private static final String SNAPSHOT_ID = "snapshotId";
    private static final String MANIFEST_ENTRIES = "manifestEntries";
    private static final String BUCKET_PREFIX = "bucket-";

    public static final PartitionedManifestMetaSerializer INSTANCE =
            new PartitionedManifestMetaSerializer();

    @Override
    public void serialize(PartitionedManifestMeta partitionedManifestMeta, JsonGenerator generator)
            throws IOException {
        DataFileMetaSerializer metaSerializer = new DataFileMetaSerializer();
        Base64.Encoder encoder = Base64.getEncoder();
        generator.writeStartObject();
        generator.writeFieldName(SNAPSHOT_ID);
        generator.writeNumber(partitionedManifestMeta.getSnapshotId());
        generator.writeObjectFieldStart(MANIFEST_ENTRIES);
        for (Map.Entry<BinaryRowData, Map<Integer, List<DataFileMeta>>> partEntry :
                partitionedManifestMeta.getManifestEntries().entrySet()) {
            generator.writeFieldName(
                    encoder.encodeToString(
                            SerializationUtils.serializeBinaryRow(partEntry.getKey())));
            generator.writeStartObject();
            for (Map.Entry<Integer, List<DataFileMeta>> bucketEntry :
                    partEntry.getValue().entrySet()) {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                metaSerializer.serializeList(
                        bucketEntry.getValue(), new DataOutputViewStreamWrapper(out));
                generator.writeStringField(
                        BUCKET_PREFIX + bucketEntry.getKey(),
                        encoder.encodeToString(out.toByteArray()));
            }
            generator.writeEndObject();
        }
        generator.writeEndObject();
        generator.writeEndObject();
    }

    @Override
    public PartitionedManifestMeta deserialize(JsonNode node) throws IOException {
        DataFileMetaSerializer metaSerializer = new DataFileMetaSerializer();
        Base64.Decoder decoder = Base64.getDecoder();
        Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> manifestEntries = new HashMap<>();
        Long snapshotId = node.get(SNAPSHOT_ID).asLong();

        JsonNode manifestEntriesNode = node.get(MANIFEST_ENTRIES);
        Iterator<Map.Entry<String, JsonNode>> partIter = manifestEntriesNode.fields();
        while (partIter.hasNext()) {
            Map.Entry<String, JsonNode> bucketNodeEntry = partIter.next();
            BinaryRowData partition =
                    SerializationUtils.deserializeBinaryRow(
                            decoder.decode(bucketNodeEntry.getKey()));
            Map<Integer, List<DataFileMeta>> bucketEntries = new HashMap<>();

            Iterator<Map.Entry<String, JsonNode>> bucketIter = bucketNodeEntry.getValue().fields();
            while (bucketIter.hasNext()) {
                Map.Entry<String, JsonNode> manifestNodeEntry = bucketIter.next();
                Integer bucket =
                        Integer.parseInt(manifestNodeEntry.getKey().replace(BUCKET_PREFIX, ""));
                bucketEntries.put(
                        bucket,
                        metaSerializer.deserializeList(
                                new DataInputViewStreamWrapper(
                                        new ByteArrayInputStream(
                                                decoder.decode(
                                                        manifestNodeEntry.getValue().asText())))));
            }
            manifestEntries.put(partition, bucketEntries);
        }
        return new PartitionedManifestMeta(snapshotId, manifestEntries);
    }
}
