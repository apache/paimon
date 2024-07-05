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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarCharType;

import org.junit.Assert;
import org.junit.Test;

/** IT cases for {@link UpdatedDataFieldsProcessFunctionBaseTest}. */
public class UpdatedDataFieldsProcessFunctionBaseTest {

    @Test
    public void testCanConvertString() {
        VarCharType oldVarchar = new VarCharType(true, 10);
        VarCharType biggerLengthVarchar = new VarCharType(true, 20);
        VarCharType smallerLengthVarchar = new VarCharType(true, 5);

        UpdatedDataFieldsProcessFunctionBase.ConvertAction convertAction = null;
        convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(oldVarchar, biggerLengthVarchar);
        Assert.assertEquals(
                UpdatedDataFieldsProcessFunctionBase.ConvertAction.CONVERT, convertAction);
        convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(oldVarchar, smallerLengthVarchar);

        Assert.assertEquals(
                UpdatedDataFieldsProcessFunctionBase.ConvertAction.IGNORE, convertAction);
    }

    @Test
    public void testCanConvertNumber() {
        IntType oldType = new IntType();
        BigIntType bigintType = new BigIntType();
        SmallIntType smallintType = new SmallIntType();

        UpdatedDataFieldsProcessFunctionBase.ConvertAction convertAction = null;
        convertAction = UpdatedDataFieldsProcessFunctionBase.canConvert(oldType, bigintType);
        Assert.assertEquals(
                UpdatedDataFieldsProcessFunctionBase.ConvertAction.CONVERT, convertAction);
        convertAction = UpdatedDataFieldsProcessFunctionBase.canConvert(oldType, smallintType);

        Assert.assertEquals(
                UpdatedDataFieldsProcessFunctionBase.ConvertAction.IGNORE, convertAction);
    }

    @Test
    public void testCanConvertDecimal() {
        DecimalType oldType = new DecimalType(20, 9);
        DecimalType biggerRangeType = new DecimalType(30, 10);
        DecimalType smallerRangeType = new DecimalType(10, 3);

        UpdatedDataFieldsProcessFunctionBase.ConvertAction convertAction = null;
        convertAction = UpdatedDataFieldsProcessFunctionBase.canConvert(oldType, biggerRangeType);
        Assert.assertEquals(
                UpdatedDataFieldsProcessFunctionBase.ConvertAction.CONVERT, convertAction);
        convertAction = UpdatedDataFieldsProcessFunctionBase.canConvert(oldType, smallerRangeType);

        Assert.assertEquals(
                UpdatedDataFieldsProcessFunctionBase.ConvertAction.IGNORE, convertAction);
    }

    @Test
    public void testCanConvertTimestamp() {
        TimestampType oldType = new TimestampType(true, 3);
        TimestampType biggerLengthTimestamp = new TimestampType(true, 5);
        TimestampType smallerLengthTimestamp = new TimestampType(true, 2);

        UpdatedDataFieldsProcessFunctionBase.ConvertAction convertAction = null;
        convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(oldType, biggerLengthTimestamp);
        Assert.assertEquals(
                UpdatedDataFieldsProcessFunctionBase.ConvertAction.CONVERT, convertAction);
        convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(oldType, smallerLengthTimestamp);

        Assert.assertEquals(
                UpdatedDataFieldsProcessFunctionBase.ConvertAction.IGNORE, convertAction);
    }
}
