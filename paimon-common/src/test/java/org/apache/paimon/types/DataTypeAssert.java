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

package org.apache.paimon.types;

import org.apache.paimon.utils.InstantiationUtil;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Condition;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.Assertions.not;

/** Assertions for {@link DataType}. */
public class DataTypeAssert extends AbstractAssert<DataTypeAssert, DataType> {

    public DataTypeAssert(DataType dataType) {
        super(dataType, DataTypeAssert.class);
    }

    public DataTypeAssert isNullable() {
        satisfies(NULLABLE);
        return this;
    }

    public DataTypeAssert isNotNullable() {
        satisfies(not(NULLABLE));
        return this;
    }

    public DataTypeAssert isJavaSerializable() {
        isNotNull();
        try {
            assertThat(
                            InstantiationUtil.<DataType>deserializeObject(
                                    InstantiationUtil.serializeObject(this.actual),
                                    DataTypesTest.class.getClassLoader()))
                    .isEqualTo(this.actual);
        } catch (IOException | ClassNotFoundException e) {
            fail(
                    "Error when trying to serialize logical type "
                            + this.actual.asSQLString()
                            + " to string",
                    e);
        }
        return myself;
    }

    public DataTypeAssert hasSQLString(String sqlString) {
        isNotNull();
        assertThat(this.actual.asSQLString()).isEqualTo(sqlString);
        return myself;
    }

    /** Tests if a {@link DataType} is nullable. */
    public static final Condition<DataType> NULLABLE =
            new Condition<>(DataType::isNullable, "nullable");
}
