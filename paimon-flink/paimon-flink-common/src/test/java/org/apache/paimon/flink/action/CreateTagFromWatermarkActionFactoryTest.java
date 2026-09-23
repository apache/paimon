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

package org.apache.paimon.flink.action;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CreateTagFromWatermarkActionFactory#create}. */
public class CreateTagFromWatermarkActionFactoryTest extends ActionITCaseBase {

    @Test
    public void testMissingWatermarkReportsRequiredArgument() {
        assertThatThrownBy(
                        () ->
                                createAction(
                                        CreateTagFromWatermarkAction.class,
                                        "create_tag_from_watermark",
                                        "--warehouse",
                                        warehouse,
                                        "--database",
                                        database,
                                        "--table",
                                        tableName,
                                        "--tag",
                                        "tag_1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Argument 'watermark' is required");
    }

    @Test
    public void testMissingTagReportsRequiredArgument() {
        assertThatThrownBy(
                        () ->
                                createAction(
                                        CreateTagFromWatermarkAction.class,
                                        "create_tag_from_watermark",
                                        "--warehouse",
                                        warehouse,
                                        "--database",
                                        database,
                                        "--table",
                                        tableName,
                                        "--watermark",
                                        "1000"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Argument 'tag' is required");
    }

    @Test
    public void testCreateWithTagAndWatermark() {
        assertThatCode(
                        () ->
                                assertThat(
                                                createAction(
                                                        CreateTagFromWatermarkAction.class,
                                                        "create_tag_from_watermark",
                                                        "--warehouse",
                                                        warehouse,
                                                        "--database",
                                                        database,
                                                        "--table",
                                                        tableName,
                                                        "--tag",
                                                        "tag_1",
                                                        "--watermark",
                                                        "1000"))
                                        .isNotNull())
                .doesNotThrowAnyException();
    }
}
