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

package org.apache.paimon.table.format;

import org.apache.paimon.fs.Path;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FormatDataSplit}. */
public class FormatDataSplitTest {

    @Test
    public void testSerializeAndDeserialize() throws IOException, ClassNotFoundException {
        // Create test data
        Path filePath = new Path("/test/path/file.parquet");
        RowType rowType = RowType.builder().field("id", new IntType()).build();
        long modificationTime = System.currentTimeMillis();

        // Create a predicate for testing
        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate predicate = builder.equal(0, 5);

        // Create FormatDataSplit
        FormatDataSplit split = new FormatDataSplit(filePath, 1024L, null);

        // Test Java serialization
        byte[] serialized = InstantiationUtil.serializeObject(split);
        FormatDataSplit deserialized =
                InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());

        // Verify the deserialized object
        assertThat(deserialized.filePath()).isEqualTo(split.filePath());
        assertThat(deserialized.offset()).isEqualTo(split.offset());
        assertThat(deserialized.fileSize()).isEqualTo(split.fileSize());
        assertThat(deserialized.length()).isEqualTo(split.length());

        split = new FormatDataSplit(filePath, 1024L, 100L, 512L, null);

        serialized = InstantiationUtil.serializeObject(split);
        deserialized = InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());

        // Verify the deserialized object
        assertThat(deserialized.filePath()).isEqualTo(split.filePath());
        assertThat(deserialized.offset()).isEqualTo(split.offset());
        assertThat(deserialized.fileSize()).isEqualTo(split.fileSize());
        assertThat(deserialized.length()).isEqualTo(split.length());
    }
}
