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

public class TestSplitFunction {
boolean myFunHasReturned$0;
    public void myFun(int[] a, int[] b) throws RuntimeException {
myFunHasReturned$0 = false;
        myFun_split1(a, b);
if (myFunHasReturned$0) { return; }

        myFun_split2(a, b);

        myFun_split3(a, b);
if (myFunHasReturned$0) { return; }

        myFun_split4(a, b);

        
        
    }
void myFun_split1(int[] a, int[] b) throws RuntimeException {
if (a[0] != 0) {
            a[0] += b[0];
            b[0] += a[1];
            { myFunHasReturned$0 = true; return; }
        }
}

void myFun_split2(int[] a, int[] b) throws RuntimeException {
a[1] += b[1];
b[1] += a[2];
}

void myFun_split3(int[] a, int[] b) throws RuntimeException {
if (a[2] != 0) {
            a[2] += b[2];
            b[2] += a[3];
            { myFunHasReturned$0 = true; return; }
        }
}

void myFun_split4(int[] a, int[] b) throws RuntimeException {
a[3] += b[3];
b[3] += a[4];
}

}
