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

package org.apache.paimon.flink.orphan;

import org.apache.paimon.fs.Path;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

class FlinkOrphanFilesCleanTest {

    @Test
    void logsRedactedPathsForExplicitDryRunAtInfo() {
        List<String> messages = new CopyOnWriteArrayList<>();
        Logger logger = (Logger) LogManager.getLogger(FlinkOrphanFilesClean.class);
        Level previousLevel = logger.getLevel();
        boolean previousAdditive = logger.isAdditive();
        AbstractAppender appender =
                new AbstractAppender(
                        "orphan-dry-run-test", null, null, false, Property.EMPTY_ARRAY) {
                    @Override
                    public void append(LogEvent event) {
                        messages.add(event.getMessage().getFormattedMessage());
                    }
                };

        try {
            Configurator.setLevel(logger.getName(), Level.INFO);
            logger.setAdditive(false);
            appender.start();
            logger.addAppender(appender);

            FlinkOrphanFilesClean.logOrphanCandidate(
                    true,
                    new Path("oss://table-user:table-password@bucket/table"),
                    new Path("oss://file-user:file-password@bucket/table/orphan.parquet"));
            FlinkOrphanFilesClean.logOrphanCandidate(
                    false,
                    new Path("oss://table-user:table-password@bucket/table"),
                    new Path("oss://file-user:file-password@bucket/table/deleted.parquet"));
        } finally {
            logger.removeAppender(appender);
            appender.stop();
            Configurator.setLevel(logger.getName(), previousLevel);
            logger.setAdditive(previousAdditive);
        }

        assertThat(messages)
                .containsExactly(
                        "Orphan file would be removed: table=oss://bucket/table "
                                + "path=oss://bucket/table/orphan.parquet");
    }
}
