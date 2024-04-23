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

package org.apache.paimon.fileindex;

import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.types.RowType;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.apache.paimon.utils.RandomUtil.randomBytes;
import static org.apache.paimon.utils.RandomUtil.randomString;

/** Test for {@link FileIndexFormat}. */
public class FileIndexFormatFormatTest {

    private static final Random RANDOM = new Random();

    @Test
    public void testWriteRead() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        FileIndexFormat.Writer writer = FileIndexFormat.createWriter(baos);

        Map<String, Map<String, byte[]>> indexes = new HashMap<>();
        for (int j = 0; j < RANDOM.nextInt(1000); j++) {
            String type = randomString(RANDOM.nextInt(100));
            Map<String, byte[]> typeIndex = indexes.computeIfAbsent(type, t -> new HashMap<>());
            for (int i = 0; i < RANDOM.nextInt(1000); i++) {
                typeIndex.put(
                        randomString(RANDOM.nextInt(20)), randomBytes(RANDOM.nextInt(100000)));
            }
        }

        writer.writeColumnIndexes(indexes);
        writer.close();

        byte[] indexBytes = baos.toByteArray();

        FileIndexFormat.Reader reader =
                FileIndexFormat.createReader(
                        new ByteArraySeekableStream(indexBytes), RowType.builder().build());

        for (Map.Entry<String, Map<String, byte[]>> entry : indexes.entrySet()) {
            String column = entry.getKey();
            for (String type : entry.getValue().keySet()) {
                byte[] b =
                        reader.readColumnInputStream(column, type)
                                .orElseThrow(RuntimeException::new);
                Assertions.assertThat(b).containsExactly(indexes.get(column).get(type));
            }
        }
    }
}
