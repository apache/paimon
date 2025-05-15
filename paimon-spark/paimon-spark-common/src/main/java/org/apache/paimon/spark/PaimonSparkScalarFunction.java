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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.types.DataType;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/** Scalar function for Spark. */
public class PaimonSparkScalarFunction implements ScalarFunction<Object>, Serializable {
    private static final long serialVersionUID = 1L;
    private final String functionName;
    private final List<DataType> inputTypes;
    private final DataType resultType;
    private String javaType;
    private String lambdaExpression;
    private transient Method compiledMethod;

    public PaimonSparkScalarFunction(
            String functionName,
            List<DataType> inputTypes,
            DataType resultType,
            String javaType,
            String lambdaExpression) {
        this.functionName = functionName;
        this.inputTypes = inputTypes;
        this.resultType = resultType;
        this.javaType = javaType;
        this.lambdaExpression = lambdaExpression;
    }

    @Override
    public DataType[] inputTypes() {
        return inputTypes.toArray(new DataType[inputTypes.size()]);
    }

    @Override
    public DataType resultType() {
        return resultType;
    }

    @Override
    public Object produceResult(InternalRow input) {
        try {
            if (this.compiledMethod == null) {
                this.compiledMethod =
                        JavaLambdaStringToMethodConverter.compileAndLoadMethod(
                                functionName, lambdaExpression, javaType);
            }
            List<Object> parameters = new ArrayList<>();
            for (int i = 0; i < inputTypes().length; i++) {
                if (inputTypes()[i] == StringType) {
                    parameters.add(input.getString(i));
                } else if (inputTypes()[i] == IntegerType) {
                    parameters.add(input.getInt(i));
                }
            }
            return this.compiledMethod.invoke(
                    null, parameters.toArray(new Object[parameters.size()]));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String name() {
        return functionName;
    }

    @Override
    public String canonicalName() {
        return functionName;
    }
}
