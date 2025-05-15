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

package org.apache.paimon.spark;

import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** paimon spark java lambda scalar function. */
public class LambdaScalarFunction implements UnboundFunction, Serializable {

    private static final long serialVersionUID = 1L;

    private DataType outputType;
    private String javaType;
    private String functionName;
    private String lambdaExpression;

    public LambdaScalarFunction(
            String functionName, DataType outputType, String javaType, String lambdaExpression) {
        try {
            this.outputType = outputType;
            this.javaType = javaType;
            this.functionName = functionName;
            this.lambdaExpression = lambdaExpression;
        } catch (Exception e) {
            throw new RuntimeException("Failed to compile lambda expression", e);
        }
    }

    @Override
    public BoundFunction bind(StructType inputType) {
        try {
            List<DataType> inputTypes = new ArrayList<>();
            for (StructField field : inputType.fields()) {
                inputTypes.add(field.dataType());
            }
            return new PaimonSparkScalarFunction(
                    functionName, inputTypes, outputType, javaType, this.lambdaExpression);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String description() {
        return "";
    }

    @Override
    public String name() {
        return functionName;
    }
}
