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

package org.apache.paimon.utils;

import org.apache.paimon.options.Options;

import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.OutputStreamAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link HadoopUtils#getHadoopConfiguration} forwards {@code hadoop.*} credentials to
 * the Hadoop configuration without leaking their values into the (DEBUG) logs.
 */
class HadoopUtilsSecretLoggingTest {

    private static final String SECRET_MARKER = "UNIQUE-SECRET-MARKER-9f8e7d6c";

    @Test
    void testCredentialValueIsNotLogged() {
        Options options = new Options();
        // hadoop.* keys are forwarded (prefix stripped) into the Hadoop configuration.
        options.set("hadoop.fs.oss.accessKeySecret", SECRET_MARKER);
        options.set("hadoop.fs.oss.endpoint", "mock-endpoint.example.com");

        // Warm up: first Hadoop Configuration build reconfigures log4j2 and drops our appender.
        HadoopUtils.getHadoopConfiguration(new Options());

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        OutputStreamAppender appender =
                OutputStreamAppender.newBuilder()
                        .setName("hadoop-utils-secret-test")
                        .setTarget(output)
                        .setLayout(PatternLayout.newBuilder().withPattern("%level %msg%n").build())
                        .build();
        Logger logger = (Logger) LogManager.getLogger(HadoopUtils.class);
        Level previousLevel = logger.getLevel();

        appender.start();
        logger.addAppender(appender);
        logger.setLevel(Level.DEBUG);
        try {
            Configuration conf = HadoopUtils.getHadoopConfiguration(options);

            // The credential must still be forwarded so the configuration keeps working.
            assertThat(conf.get("fs.oss.accessKeySecret")).isEqualTo(SECRET_MARKER);

            String logs = new String(output.toByteArray(), StandardCharsets.UTF_8);
            // The key name may appear, but the secret value must never be logged.
            assertThat(logs).contains("fs.oss.accessKeySecret");
            assertThat(logs).doesNotContain(SECRET_MARKER);
        } finally {
            logger.setLevel(previousLevel);
            logger.removeAppender(appender);
            appender.stop();
        }
    }
}
