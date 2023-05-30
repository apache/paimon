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

public class TestRewriteReturnValue {
String fun1ReturnValue$0;
String fun2ReturnValue$1;

    public String fun1(int a, String b) { fun1Impl(a, b); return fun1ReturnValue$0; }

void fun1Impl(int a, String b) {
        if (a > 0) {
            a += 5;
            { fun1ReturnValue$0 = b + "test" + a; return; }
        }
        a -= 5;
        { fun1ReturnValue$0 = b + "test" + a; return; }
    }

    public String fun2(int a, String b) throws Exception { fun2Impl(a, b); return fun2ReturnValue$1; }

void fun2Impl(int a, String b) throws Exception {
        if (a > 0) {
            a += 5;
            if (a > 100) {
                throw new RuntimeException();
            }
            { fun2ReturnValue$1 = b + "test" + a; return; }
        }
        a -= 5;
        { fun2ReturnValue$1 = b + "test" + a; return; }
    }
}
