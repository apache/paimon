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

package org.apache.paimon.jindo;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.apache.paimon.rest.RESTTokenFileIO.DLF_ACCESS_TRACKING_EXTENDED_INFO;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for DLF access tracking extended info in {@link JindoFileIO}. */
public class TestJindoDlfAccessTracking {

    private static final String OSS_USER_AGENT_EXTENDED = "fs.oss.user.agent.extended";

    private Options getHadoopOptions(JindoFileIO fileIO) {
        try {
            Field field = JindoFileIO.class.getDeclaredField("hadoopOptions");
            field.setAccessible(true);
            return (Options) field.get(fileIO);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Failed to access hadoopOptions field", e);
        }
    }

    @Test
    public void testExtendedInfoSetToUserAgent() {
        Options options = new Options();
        options.set(DLF_ACCESS_TRACKING_EXTENDED_INFO, "tracking-info-123");
        options.set("fs.oss.accessKeyId", "testAk");
        options.set("fs.oss.accessKeySecret", "testSk");

        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(CatalogContext.create(options));

        Options hadoopOptions = getHadoopOptions(fileIO);
        assertThat(hadoopOptions.get(OSS_USER_AGENT_EXTENDED)).isEqualTo("tracking-info-123");
    }

    @Test
    public void testExtendedInfoAppendedToExistingUserAgent() {
        Options options = new Options();
        options.set(DLF_ACCESS_TRACKING_EXTENDED_INFO, "tracking-info-456");
        options.set(OSS_USER_AGENT_EXTENDED, "existing-agent");
        options.set("fs.oss.accessKeyId", "testAk");
        options.set("fs.oss.accessKeySecret", "testSk");

        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(CatalogContext.create(options));

        Options hadoopOptions = getHadoopOptions(fileIO);
        assertThat(hadoopOptions.get(OSS_USER_AGENT_EXTENDED))
                .isEqualTo("existing-agent tracking-info-456");
    }

    @Test
    public void testNoExtendedInfoWhenNotConfigured() {
        Options options = new Options();
        options.set("fs.oss.accessKeyId", "testAk");
        options.set("fs.oss.accessKeySecret", "testSk");

        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(CatalogContext.create(options));

        Options hadoopOptions = getHadoopOptions(fileIO);
        assertThat(hadoopOptions.get(OSS_USER_AGENT_EXTENDED)).isNull();
    }

    @Test
    public void testEmptyExtendedInfoIgnored() {
        Options options = new Options();
        options.set(DLF_ACCESS_TRACKING_EXTENDED_INFO, "  ");
        options.set("fs.oss.accessKeyId", "testAk");
        options.set("fs.oss.accessKeySecret", "testSk");

        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(CatalogContext.create(options));

        Options hadoopOptions = getHadoopOptions(fileIO);
        assertThat(hadoopOptions.get(OSS_USER_AGENT_EXTENDED)).isNull();
    }
}
