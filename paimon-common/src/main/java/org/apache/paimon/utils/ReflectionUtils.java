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

package org.apache.paimon.utils;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/** Utils for java reflection. */
public class ReflectionUtils {

    @SuppressWarnings("rawtypes")
    public static <T> T invokeStaticMethod(Class clz, String methodName, Object... args)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method method = null;
        Method[] methods = clz.getDeclaredMethods();
        for (Method m : methods) {
            if (methodName.equals(m.getName())) {
                method = m;
                break;
            }
        }

        if (method == null) {
            throw new NoSuchMethodException(methodName);
        }
        return invokeStaticMethod(method, args);
    }

    @SuppressWarnings("unchecked")
    public static <T> T invokeStaticMethod(Method method, Object... args)
            throws InvocationTargetException, IllegalAccessException {
        return (T) method.invoke(null, args);
    }

    public static Method getMethod(Class<?> clz, String methodName, int argSize)
            throws NoSuchMethodException {
        Method method = null;
        Method[] methods = clz.getMethods();
        for (Method m : methods) {
            if (methodName.equals(m.getName()) && m.getParameterTypes().length == argSize) {
                method = m;
                break;
            }
        }

        if (method == null) {
            throw new NoSuchMethodException(methodName);
        }
        return method;
    }

    @SuppressWarnings("unchecked")
    public static <T> T getPrivateFieldValue(Object obj, String fieldName)
            throws NoSuchFieldException, IllegalAccessException {
        Class<?> clazz = obj.getClass();
        while (clazz != null) {
            try {
                Field field = clazz.getDeclaredField(fieldName);
                field.setAccessible(true);
                return (T) field.get(obj);
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }

    @SuppressWarnings("unchecked")
    public static <T> T invokeMethod(Object obj, String methodName, Object... args)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Class<?> clazz = obj.getClass();
        while (clazz != null) {
            try {
                Method[] methods = clazz.getDeclaredMethods();
                for (Method m : methods) {
                    if (methodName.equals(m.getName())
                            && m.getParameterTypes().length == args.length) {
                        m.setAccessible(true);
                        return (T) m.invoke(obj, args);
                    }
                }
                // Try public methods
                Method[] publicMethods = clazz.getMethods();
                for (Method m : publicMethods) {
                    if (methodName.equals(m.getName())
                            && m.getParameterTypes().length == args.length) {
                        return (T) m.invoke(obj, args);
                    }
                }
                clazz = clazz.getSuperclass();
            } catch (NoSuchMethodException e) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new NoSuchMethodException(methodName + " with " + args.length + " parameters");
    }
}
