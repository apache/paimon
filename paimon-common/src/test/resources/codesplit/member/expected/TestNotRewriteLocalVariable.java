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

public class TestNotRewriteLocalVariable {
int[] rewrite$0 = new int[3];

{
rewrite$0[0] = 1;
rewrite$0[1] = 2;
rewrite$0[2] = 3;
}

    
    
    

    public TestNotRewriteFunctionParameter() {
        int b = this.rewrite$0[1];
        int c = this.rewrite$0[2];
        System.out.println(this.rewrite$0[2] + b + c + this.rewrite$0[1]);
    }

    public int myFun() {
        int a = this.rewrite$0[0];
        return this.rewrite$0[0] + a + this.rewrite$0[1] + rewrite$0[2];
    }
}
