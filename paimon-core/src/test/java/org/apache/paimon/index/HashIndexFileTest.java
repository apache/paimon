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

package org.apache.paimon.index;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.utils.IntIterator;
import org.apache.paimon.utils.PathFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link HashIndexFile}. */
public class HashIndexFileTest {

    @TempDir java.nio.file.Path tempPath;

    @Test
    public void test() throws IOException {
        Path dir = new Path(tempPath.toUri());
        PathFactory pathFactory =
                new PathFactory() {
                    @Override
                    public Path newPath() {
                        return new Path(dir, UUID.randomUUID().toString());
                    }

                    @Override
                    public Path toPath(String fileName) {
                        return new Path(dir, fileName);
                    }
                };

        HashIndexFile file = new HashIndexFile(LocalFileIO.create(), pathFactory);

        Random rnd = new Random();
        List<Integer> random = new ArrayList<>();
        for (int i = 0; i < rnd.nextInt(100_000); i++) {
            random.add(rnd.nextInt());
        }

        String name =
                file.write(
                        IntIterator.create(random.stream().mapToInt(Integer::intValue).toArray()));

        List<Integer> result = IntIterator.toIntList(file.read(name));
        assertThat(result).containsExactlyInAnyOrderElementsOf(random);

        assertThat(file.fileSize(name)).isEqualTo(random.size() * 4L);
    }
}
