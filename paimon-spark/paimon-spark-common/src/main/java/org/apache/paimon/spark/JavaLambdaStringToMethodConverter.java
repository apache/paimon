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

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Util class for compiling and loading Java lambda expressions as methods. */
public class JavaLambdaStringToMethodConverter {
    private static final Pattern LAMBDA_PATTERN =
            Pattern.compile("\\s*\\(([^)]*)\\)\\s*->\\s*(.+)\\s*");

    public static String getClassName(String functionName) {
        return "PaimonLambdaFunction" + functionName;
    }

    public static String getSourceFileName(String functionName) {
        return String.format("%s.java", getClassName(functionName));
    }

    public static Method compileAndLoadMethod(
            String functionName, String lambdaExpression, String returnType) throws Exception {
        String className = getClassName(functionName);
        String fullMethod = parseLambdaWithType(returnType, lambdaExpression, "apply");

        String javaCode = "public class " + className + " { " + fullMethod + " }";

        File sourceFile = new File(getSourceFileName(functionName));
        try (FileWriter writer = new FileWriter(sourceFile)) {
            writer.write(javaCode);
        }

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, null, null);
        Iterable<? extends JavaFileObject> compilationUnits =
                fileManager.getJavaFileObjectsFromFiles(Arrays.asList(sourceFile));
        compiler.getTask(null, fileManager, null, null, null, compilationUnits).call();
        fileManager.close();
        URLClassLoader classLoader =
                URLClassLoader.newInstance(new URL[] {new File(".").toURI().toURL()});
        Class<?> compiledClass = Class.forName(className, true, classLoader);
        return compiledClass.getDeclaredMethods()[0];
    }

    public static String parseLambdaWithType(
            String outputType, String expression, String methodName) {
        Matcher matcher = LAMBDA_PATTERN.matcher(expression);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid lambda expression: " + expression);
        }

        String parameters = matcher.group(1).trim();
        String body = matcher.group(2).trim();

        StringBuilder method = new StringBuilder();
        method.append("public static ")
                .append(outputType)
                .append(" ")
                .append(methodName)
                .append("(")
                .append(parameters)
                .append(")");

        if (!body.startsWith("{")) {
            method.append("{");
        }
        if (!body.contains("return ")) {
            method.append("return ");
        }
        if (!body.contains(";")) {
            method.append(body).append(";");
        } else {
            method.append(body);
        }
        if (!body.endsWith("}")) {
            method.append("{");
        }
        return method.toString();
    }
}
