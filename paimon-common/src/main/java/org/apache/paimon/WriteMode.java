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

package org.apache.paimon;

import org.apache.paimon.options.description.DescribedEnum;
import org.apache.paimon.options.description.InlineElement;

import static org.apache.paimon.options.description.TextElement.text;

/** Defines the write mode for paimon. */
public enum WriteMode implements DescribedEnum {
    AUTO(
            "auto",
            "The change-log for table with primary key, append-only for table without primary key."),
    APPEND_ONLY(
            "append-only",
            "The table can only accept append-only insert operations. Neither data deduplication nor any "
                    + "primary key constraints will be done when inserting rows into paimon."),
    CHANGE_LOG("change-log", "The table can accept insert/delete/update operations.");

    private final String value;
    private final String description;

    WriteMode(String value, String description) {
        this.value = value;
        this.description = description;
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public InlineElement getDescription() {
        return text(description);
    }
}
