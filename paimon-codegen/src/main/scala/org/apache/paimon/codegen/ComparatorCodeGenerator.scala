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

package org.apache.paimon.codegen

import org.apache.paimon.codegen.GenerateUtils.{newName, ROW_DATA}
import org.apache.paimon.types.RowType

/** A code generator for generating [[RecordComparator]]. */
object ComparatorCodeGenerator {

  /**
   * Generates a [[RecordComparator]] that can be passed to a Java compiler.
   *
   * @param name
   *   Class name of the function. Does not need to be unique but has to be a valid Java class
   *   identifier.
   * @param inputType
   *   input type.
   * @param sortSpec
   *   sort specification.
   * @return
   *   A GeneratedRecordComparator
   */
  def gen(
      name: String,
      inputType: RowType,
      sortSpec: SortSpec): GeneratedClass[RecordComparator] = {
    val className = newName(name)
    val baseClass = classOf[RecordComparator]

    val ctx = new CodeGeneratorContext()
    val compareCode = GenerateUtils.generateRowCompare(ctx, inputType, sortSpec, "o1", "o2")

    val code =
      s"""
      public class $className implements ${baseClass.getCanonicalName} {

        private final Object[] references;
        ${ctx.reuseMemberCode()}

        public $className(Object[] references) {
          this.references = references;
          ${ctx.reuseInitCode()}
        }

        @Override
        public int compare($ROW_DATA o1, $ROW_DATA o2) {
          $compareCode
          return 0;
        }

      }
      """.stripMargin

    new GeneratedClass(className, code, ctx.references.toArray)
  }

}
