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

public class TestAddBooleanBeforeReturn {
boolean fun2HasReturned$1;
boolean fun1HasReturned$0;
    public void fun1(int a) {
        if (a > 0) {
            a += 5;
            { fun1HasReturned$0 = true; return; }
        }
        a -= 5;
        { fun1HasReturned$0 = true; return; }
    }

    public void fun2(String b) {
        if (b.length() > 5) {
            b += "b";
            { fun2HasReturned$1 = true; return; }
        }
        b += "a";
        { fun2HasReturned$1 = true; return; }
    }
}
