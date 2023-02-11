/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.format.orc;

import org.apache.flink.table.store.options.Options;

import org.junit.jupiter.api.Test;

import static org.apache.flink.table.store.format.orc.OrcFileFormatFactory.IDENTIFIER;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link OrcFileFormatFactory}. */
public class OrcFileFormatTest {

    @Test
    public void testAbsent() {
        Options options = new Options();
        options.setString("haha", "1");
        OrcFileFormat orc = new OrcFileFormatFactory().create(options);
        assertThat(orc.orcProperties().getProperty(IDENTIFIER + ".haha", "")).isEqualTo("1");
        assertThat(orc.orcProperties().getProperty(IDENTIFIER + ".compress", "")).isEqualTo("lz4");
    }

    @Test
    public void testPresent() {
        Options options = new Options();
        options.setString("haha", "1");
        options.setString("compress", "zlib");
        OrcFileFormat orc = new OrcFileFormatFactory().create(options);
        assertThat(orc.orcProperties().getProperty(IDENTIFIER + ".haha", "")).isEqualTo("1");
        assertThat(orc.orcProperties().getProperty(IDENTIFIER + ".compress", "")).isEqualTo("zlib");
    }
}
