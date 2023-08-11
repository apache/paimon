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

package org.apache.paimon.flink.action.cdc;

import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import javax.annotation.Nullable;

import java.io.Serializable;

import static org.apache.paimon.flink.action.cdc.mysql.MySqlTypeUtils.TIME;
import static org.apache.paimon.flink.action.cdc.mysql.MySqlTypeUtils.TINYINT;

/** Utility class that holds some special MySQL type to Paimon type cast rules . */
public class SpecialCastRules implements Serializable {

    /**
     * MySQL haven't boolean type, it uses tinyint(1) to represents boolean type user should not use
     * tinyint(1) to store number although jdbc url parameter tinyInt1isBit=false can help change
     * the return value, it's not a general way. mybatis and mysql-connector-java map tinyint(1) to
     * boolean by default, we behave the same way by default. To store number (-128~127), we can set
     * the parameter tinyInt1ToByte (option 'mysql.converter.tinyint1-to-bool') to false, then
     * tinyint(1) will be mapped to TinyInt.
     */
    private final boolean castTinyInt1ToBool;

    /** Hive doesn't support TIME, so here convert it into STRING type. */
    private final boolean castTimeToString;

    public SpecialCastRules(boolean castTinyInt1ToBool, boolean castTimeToString) {
        this.castTinyInt1ToBool = castTinyInt1ToBool;
        this.castTimeToString = castTimeToString;
    }

    public static SpecialCastRules defaultRules() {
        return new SpecialCastRules(true, false);
    }

    public DataType cast(String type, @Nullable Integer length) {
        switch (type) {
            case TINYINT:
                return castTinyInt1ToBool && length != null && length == 1
                        ? DataTypes.BOOLEAN()
                        : DataTypes.TINYINT();

            case TIME:
                return castTimeToString ? DataTypes.STRING() : DataTypes.TIME();
            default:
                throw new IllegalArgumentException("No special cast rule for type: " + type);
        }
    }
}
