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

public class TestRewriteInnerClass {
String funReturnValue$0;

    public String fun(int a, String b) { funImpl(a, b); return funReturnValue$0; }

void funImpl(int a, String b) {
        if (a > 0) {
            a += 5;
            { funReturnValue$0 = b + "test" + a; return; }
        }
        a -= 5;
        { funReturnValue$0 = b + "test" + a; return; }
    }

    public class InnerClass {
String funReturnValue$1;

        public String fun(int a, String b) { funImpl(a, b); return funReturnValue$1; }

void funImpl(int a, String b) {
            if (a > 0) {
                a += 5;
                { funReturnValue$1 = b + "test" + a; return; }
            }
            a -= 5;
            { funReturnValue$1 = b + "test" + a; return; }
        }
    }
}
