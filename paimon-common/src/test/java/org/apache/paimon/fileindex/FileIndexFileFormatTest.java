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
import org.apache.paimon.fs.SeekableInputStream;
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

/** Test for {@link FileIndexFile}. */
public class FileIndexFileFormatTest {

    private static final Random RANDOM = new Random();

    @Test
    public void testWriteRead() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        FileIndexFile.Writer writer = FileIndexFile.createWriter(baos);

        String type = randomString(RANDOM.nextInt(100));
        Map<String, byte[]> indexes = new HashMap<>();
        for (int i = 0; i < RANDOM.nextInt(1000); i++) {
            indexes.put(randomString(RANDOM.nextInt(20)), randomBytes(RANDOM.nextInt(100000)));
        }

        writer.writeColumnIndex(type, indexes);
        writer.close();

        byte[] indexBytes = baos.toByteArray();

        FileIndexFile.Reader reader =
                FileIndexFile.createReader(
                        new ByteArraySeekableStream(indexBytes), RowType.builder().build());

        for (String s : indexes.keySet()) {
            SeekableInputStream seekableInputStream =
                    reader.readColumnInputStream(s).orElseThrow(RuntimeException::new);
            byte[] b = new byte[seekableInputStream.available()];
            int i = seekableInputStream.read(b);
            Assertions.assertThat(i).isEqualTo(b.length);
            Assertions.assertThat(b).containsExactly(indexes.get(s));
        }
    }
}
