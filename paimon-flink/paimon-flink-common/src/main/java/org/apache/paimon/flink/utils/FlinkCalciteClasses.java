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

package org.apache.paimon.flink.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

/**
 * Class for load calcite dependency via reflection. This is because Flink hides Calcite related
 * dependencies in a runtime classloader. What this class needs to do is extract the Calcite class
 * from this special classloader, but it cannot explicitly rely on them and can only be called
 * through reflection.
 */
public class FlinkCalciteClasses {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkCalciteClasses.class);

    private static final String Flink_PLANNER_MODULE_CLASS =
            "org.apache.flink.table.planner.loader.PlannerModule";
    private static final String PLANNER_MODULE_METHOD = "getInstance";
    private static final String SUBMODULE_CLASS_LOADER = "submoduleClassLoader";

    private static final ClassLoader submoduleClassLoader;

    private final SqlNodeListDelegate sqlNodeListDelegate;
    private final SqlLiteralDelegate sqlLiteralDelegate;
    private final SqlBasicCallDelegate sqlBasicCallDelegate;
    private final SqlOperatorDelegate sqlOperatorDelegate;
    private final SqlKindDelegate sqlKindDelegate;
    private final SqlParserDelegate sqlParserDelegate;
    private final LexDelegate lexDelegate;
    private final ConfigDelegate configDelegate;
    private final SqlIdentifierDelegate sqlIdentifierDelegate;

    static {
        boolean calciteFound = false;
        ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            // this code path is just for testing
            currentClassLoader.loadClass(SqlParserDelegate.CLASS_NAME);
            calciteFound = true;
        } catch (ClassNotFoundException ignored) {
        }

        try {
            if (calciteFound) {
                submoduleClassLoader = currentClassLoader;
            } else {
                submoduleClassLoader = initCalciteClassLoader();
            }
        } catch (Exception e) {
            LOG.error(String.format("Load Calcite class Fail: %s", e.getMessage()), e);
            throw new RuntimeException(e);
        }
    }

    public FlinkCalciteClasses() throws ClassNotFoundException {
        sqlNodeListDelegate = new SqlNodeListDelegate();
        sqlLiteralDelegate = new SqlLiteralDelegate();
        sqlBasicCallDelegate = new SqlBasicCallDelegate();
        sqlOperatorDelegate = new SqlOperatorDelegate();
        sqlKindDelegate = new SqlKindDelegate();
        sqlParserDelegate = new SqlParserDelegate();
        lexDelegate = new LexDelegate();
        configDelegate = new ConfigDelegate();
        sqlIdentifierDelegate = new SqlIdentifierDelegate();
    }

    private static ClassLoader initCalciteClassLoader() throws Exception {
        Class<?> plannerModuleClass = Class.forName(Flink_PLANNER_MODULE_CLASS);
        Method getInstanceMethod = plannerModuleClass.getDeclaredMethod(PLANNER_MODULE_METHOD);
        getInstanceMethod.setAccessible(true);
        Object plannerModuleInstance = getInstanceMethod.invoke(null);

        Field submoduleClassLoaderField =
                plannerModuleClass.getDeclaredField(SUBMODULE_CLASS_LOADER);
        submoduleClassLoaderField.setAccessible(true);
        return (ClassLoader) submoduleClassLoaderField.get(plannerModuleInstance);
    }

    private static Object invokeMethod(
            Class<?> clazz, Object object, String methodName, Class<?>[] argsClass, Object[] args)
            throws Exception {
        Method method = clazz.getMethod(methodName, argsClass);
        method.setAccessible(true);
        return method.invoke(object, args);
    }

    private static Class<?> loadCalciteClass(String className) throws ClassNotFoundException {
        return Class.forName(className, true, submoduleClassLoader);
    }

    /** Accessing org.apache.calcite.sql.parser.SqlParser by Reflection. */
    public static class SqlParserDelegate {
        private static final String CLASS_NAME = "org.apache.calcite.sql.parser.SqlParser";
        private final Class<?> clazz;

        public SqlParserDelegate() throws ClassNotFoundException {
            clazz = loadCalciteClass(CLASS_NAME);
        }

        public Object config() throws Exception {
            return invokeMethod(clazz, null, "config", new Class[0], new Object[0]);
        }

        public Object create(String whereSql, Object config) throws Exception {
            return invokeMethod(
                    clazz,
                    null,
                    "create",
                    new Class[] {String.class, loadCalciteClass(ConfigDelegate.CLASS_NAME)},
                    new Object[] {whereSql, config});
        }

        public Object parseExpression(Object sqlParser) throws Exception {
            return invokeMethod(clazz, sqlParser, "parseExpression", new Class[0], new Object[0]);
        }
    }

    /** Accessing org.apache.calcite.config.Lex by Reflection. */
    public static class LexDelegate {
        private static final String CLASS_NAME = "org.apache.calcite.config.Lex";
        private final Class<?> clazz;

        public LexDelegate() throws ClassNotFoundException {
            this.clazz = loadCalciteClass(CLASS_NAME);
        }

        public Object java() throws NoSuchFieldException, IllegalAccessException {
            return clazz.getField("JAVA").get(null);
        }
    }

    /** Accessing org.apache.calcite.sql.SqlKind by Reflection. */
    public static class SqlKindDelegate {
        private static final String CLASS_NAME = "org.apache.calcite.sql.SqlKind";
        private final Class<?> clazz;

        public SqlKindDelegate() throws ClassNotFoundException {
            clazz = loadCalciteClass(CLASS_NAME);
        }

        public Object or() throws NoSuchFieldException, IllegalAccessException {
            return clazz.getField("OR").get(null);
        }

        public Object and() throws NoSuchFieldException, IllegalAccessException {
            return clazz.getField("AND").get(null);
        }

        public Object equals() throws NoSuchFieldException, IllegalAccessException {
            return clazz.getField("EQUALS").get(null);
        }

        public Object notEquals() throws NoSuchFieldException, IllegalAccessException {
            return clazz.getField("NOT_EQUALS").get(null);
        }

        public Object lessThan() throws NoSuchFieldException, IllegalAccessException {
            return clazz.getField("LESS_THAN").get(null);
        }

        public Object lessThanOrEqual() throws NoSuchFieldException, IllegalAccessException {
            return clazz.getField("LESS_THAN_OR_EQUAL").get(null);
        }

        public Object greaterThan() throws NoSuchFieldException, IllegalAccessException {
            return clazz.getField("GREATER_THAN").get(null);
        }

        public Object greaterThanOrEqual() throws NoSuchFieldException, IllegalAccessException {
            return clazz.getField("GREATER_THAN_OR_EQUAL").get(null);
        }

        public Object in() throws NoSuchFieldException, IllegalAccessException {
            return clazz.getField("IN").get(null);
        }

        public Object isNull() throws NoSuchFieldException, IllegalAccessException {
            return clazz.getField("IS_NULL").get(null);
        }

        public Object isNotNull() throws NoSuchFieldException, IllegalAccessException {
            return clazz.getField("IS_NOT_NULL").get(null);
        }

        public Object not() throws NoSuchFieldException, IllegalAccessException {
            return clazz.getField("NOT").get(null);
        }
    }

    /** Accessing org.apache.calcite.sql.parser.SqlParser$Config by Reflection. */
    public static class ConfigDelegate {
        static final String CLASS_NAME = "org.apache.calcite.sql.parser.SqlParser$Config";

        public Object withLex(Object config, Object lex) throws Exception {
            return invokeMethod(
                    config.getClass(),
                    config,
                    "withLex",
                    new Class[] {loadCalciteClass(LexDelegate.CLASS_NAME)},
                    new Object[] {lex});
        }
    }

    /** Accessing org.apache.calcite.sql.SqlBasicCall by Reflection. */
    public static class SqlBasicCallDelegate {
        static final String CLASS_NAME = "org.apache.calcite.sql.SqlBasicCall";
        private final Class<?> clazz;

        public SqlBasicCallDelegate() throws ClassNotFoundException {
            this.clazz = loadCalciteClass(CLASS_NAME);
        }

        public Object getOperator(Object basicCall) throws Exception {
            return invokeMethod(clazz, basicCall, "getOperator", new Class[0], new Object[0]);
        }

        public List<?> getOperandList(Object basicCall) throws Exception {
            return (List<?>)
                    invokeMethod(clazz, basicCall, "getOperandList", new Class[0], new Object[0]);
        }
    }

    /** Accessing org.apache.calcite.sql.SqlOperator by Reflection. */
    public static class SqlOperatorDelegate {
        static final String SQL_OPERATOR = "org.apache.calcite.sql.SqlOperator";
        private final Class<?> sqlOperatorClazz;

        static final String SQL_BINARY_OPERATOR = "org.apache.calcite.sql.SqlBinaryOperator";
        private final Class<?> sqlBinaryOperatorClazz;

        static final String SQL_SQL_POSTFIX_OPERATOR = "org.apache.calcite.sql.SqlPostfixOperator";
        private final Class<?> sqlPostfixOperatorClazz;

        static final String SQL_PREFIX_OPERATOR = "org.apache.calcite.sql.SqlPrefixOperator";
        private final Class<?> sqlPrefixOperatorClazz;

        public SqlOperatorDelegate() throws ClassNotFoundException {
            this.sqlOperatorClazz = loadCalciteClass(SQL_OPERATOR);
            this.sqlBinaryOperatorClazz = loadCalciteClass(SQL_BINARY_OPERATOR);
            this.sqlPostfixOperatorClazz = loadCalciteClass(SQL_SQL_POSTFIX_OPERATOR);
            this.sqlPrefixOperatorClazz = loadCalciteClass(SQL_PREFIX_OPERATOR);
        }

        public Object getKind(Object operator) throws Exception {
            return invokeMethod(sqlOperatorClazz, operator, "getKind", new Class[0], new Object[0]);
        }

        public boolean instanceOfSqlBinaryOperator(Object operator) throws Exception {
            return sqlBinaryOperatorClazz.isAssignableFrom(operator.getClass());
        }

        public boolean instanceOfSqlPostfixOperator(Object operator) throws Exception {
            return sqlPostfixOperatorClazz.isAssignableFrom(operator.getClass());
        }

        public boolean instanceOfSqlPrefixOperator(Object operator) throws Exception {
            return sqlPrefixOperatorClazz.isAssignableFrom(operator.getClass());
        }
    }

    /** Accessing org.apache.calcite.sql.SqlIdentifier by Reflection. */
    public static class SqlIdentifierDelegate {
        private static final String SQL_IDENTIFIER = "org.apache.calcite.sql.SqlIdentifier";
        private final Class<?> identifierClazz;

        public SqlIdentifierDelegate() throws ClassNotFoundException {
            this.identifierClazz = loadCalciteClass(SQL_IDENTIFIER);
        }

        public boolean instanceOfSqlIdentifier(Object sqlNode) throws Exception {
            return identifierClazz.isAssignableFrom(sqlNode.getClass());
        }
    }

    /** Accessing org.apache.calcite.sql.SqlNodeList by Reflection. */
    public static class SqlNodeListDelegate {
        private static final String SQL_NODE_LIST = "org.apache.calcite.sql.SqlNodeList";
        private final Class<?> clazz;

        public SqlNodeListDelegate() throws ClassNotFoundException {
            this.clazz = loadCalciteClass(SQL_NODE_LIST);
        }

        public List<?> getList(Object sqlNodeList) throws Exception {
            return (List<?>)
                    invokeMethod(clazz, sqlNodeList, "getList", new Class[0], new Object[0]);
        }
    }

    /** Accessing org.apache.calcite.sql.SqlLiteral by Reflection. */
    public static class SqlLiteralDelegate {
        private static final String CLASS_NAME = "org.apache.calcite.sql.SqlLiteral";
        private final Class<?> clazz;

        public SqlLiteralDelegate() throws ClassNotFoundException {
            this.clazz = loadCalciteClass(CLASS_NAME);
        }

        public boolean instanceOfSqlLiteral(Object sqlNode) throws Exception {
            return clazz.isAssignableFrom(sqlNode.getClass());
        }

        public String toValue(Object sqlNode) throws Exception {
            return (String)
                    invokeMethod(clazz, sqlNode, "toValue", new Class[] {}, new Object[] {});
        }
    }

    public SqlNodeListDelegate sqlNodeListDelegate() {
        return sqlNodeListDelegate;
    }

    public SqlLiteralDelegate sqlLiteralDelegate() {
        return sqlLiteralDelegate;
    }

    public SqlBasicCallDelegate sqlBasicCallDelegate() {
        return sqlBasicCallDelegate;
    }

    public SqlOperatorDelegate sqlOperatorDelegate() {
        return sqlOperatorDelegate;
    }

    public SqlKindDelegate sqlKindDelegate() {
        return sqlKindDelegate;
    }

    public SqlParserDelegate sqlParserDelegate() {
        return sqlParserDelegate;
    }

    public LexDelegate lexDelegate() {
        return lexDelegate;
    }

    public ConfigDelegate configDelegate() {
        return configDelegate;
    }

    public SqlIdentifierDelegate sqlIndentifierDelegate() {
        return sqlIdentifierDelegate;
    }
}
