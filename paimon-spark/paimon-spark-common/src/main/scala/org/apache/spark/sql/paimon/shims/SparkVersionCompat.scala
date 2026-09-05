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

package org.apache.spark.sql.paimon.shims

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogPlugin}

/**
 * Reflective accessors for a handful of Spark internals whose *signatures* (not just arity) changed
 * across supported versions, in ways that named accessors cannot paper over.
 *
 * Everything here is deliberately narrow: prefer a named accessor, or a per-version
 * `MinorVersionShim` method, over adding to this object. Reflection is the last resort, for the
 * cases where the same logical operation has an incompatible static type on different Spark
 * versions. It cannot be delegated to `MinorVersionShim` the way arity changes are, because that
 * object lives in the per-version modules (`paimon-spark3-common`, `paimon-spark-3.2`,
 * `paimon-spark-3.3`, ...) and those depend on `paimon-spark-common` — the dependency cannot be
 * inverted, so `paimon-spark-common` has to resolve such differences itself at runtime.
 */
object SparkVersionCompat {

  /**
   * Spark 4.2 turned `CatalogManager` from a class into an interface. Source-compatible, binary
   * incompatible in *both* directions: the compiler picks `invokevirtual` or `invokeinterface` from
   * the owner's kind, and the JVM raises `IncompatibleClassChangeError` when the two disagree.
   * Since `paimon-spark-common` is compiled once against the newest supported Spark and shipped to
   * every older 4.x runtime, a direct call would break all of them.
   *
   * Reflection is immune: only invoke opcodes carry the class/interface distinction, so
   * `Class.getMethod` resolves the same either way. These four are every `CatalogManager` member
   * `paimon-spark-common` reaches today; a fifth belongs here too. Nothing enforces that
   * automatically — `tools/spark-binary-compat/check_linkage.py` finds a direct call, but it is a
   * manual script, not wired into the build.
   */
  private lazy val currentCatalogMethod = catalogManagerMethod("currentCatalog")
  private lazy val catalogByNameMethod = catalogManagerMethod("catalog", classOf[String])
  private lazy val currentNamespaceMethod = catalogManagerMethod("currentNamespace")
  private lazy val v1SessionCatalogMethod = catalogManagerMethod("v1SessionCatalog")

  private def catalogManagerMethod(name: String, paramTypes: Class[_]*): java.lang.reflect.Method =
    classOf[CatalogManager].getMethod(name, paramTypes: _*)

  /**
   * Invokes a method reflectively, unwrapping the reflection layer so callers see exactly what a
   * direct call would have thrown. `CatalogManager.catalog` raises `CatalogNotFoundException` for
   * an unknown name and callers depend on catching it, so letting an `InvocationTargetException`
   * escape would silently change control flow. Every reflective call in this object goes through
   * here for that reason.
   */
  private def invoke[T](method: java.lang.reflect.Method, receiver: AnyRef, args: Any*): T =
    try {
      method.invoke(receiver, args.map(_.asInstanceOf[AnyRef]): _*).asInstanceOf[T]
    } catch {
      case e: java.lang.reflect.InvocationTargetException => throw e.getCause
    }

  def currentCatalog(catalogManager: CatalogManager): CatalogPlugin =
    invoke[CatalogPlugin](currentCatalogMethod, catalogManager)

  def catalog(catalogManager: CatalogManager, name: String): CatalogPlugin =
    invoke[CatalogPlugin](catalogByNameMethod, catalogManager, name)

  def currentNamespace(catalogManager: CatalogManager): Array[String] =
    invoke[Array[String]](currentNamespaceMethod, catalogManager)

  def v1SessionCatalog(catalogManager: CatalogManager): SessionCatalog =
    invoke[SessionCatalog](v1SessionCatalogMethod, catalogManager)

  // Spark 4.2 narrowed `SessionCatalog.isBuiltinFunction` from `FunctionIdentifier` to `String`,
  // dropping the database/catalog qualifier from the lookup. This accessor therefore takes a bare
  // function name: it is the only input both overloads can answer identically. Passing a qualified
  // identifier would yield `true` on 4.2 and `false` on <= 4.1 for e.g. `mydb.upper`.
  private lazy val byNameMethod: Option[java.lang.reflect.Method] =
    try {
      Some(classOf[SessionCatalog].getMethod("isBuiltinFunction", classOf[String]))
    } catch {
      case _: NoSuchMethodException => None
    }

  private lazy val byIdentMethod: Option[java.lang.reflect.Method] =
    try {
      Some(classOf[SessionCatalog].getMethod("isBuiltinFunction", classOf[FunctionIdentifier]))
    } catch {
      case _: NoSuchMethodException => None
    }

  def isBuiltinFunction(catalog: SessionCatalog, name: String): Boolean = {
    byNameMethod
      .map(invoke[java.lang.Boolean](_, catalog, name).booleanValue())
      .orElse(byIdentMethod.map(invoke[java.lang.Boolean](_, catalog, FunctionIdentifier(name))
        .booleanValue()))
      .getOrElse(throw new NoSuchMethodError(
        "SessionCatalog.isBuiltinFunction was added in Spark 3.3; found neither the String nor " +
          "the FunctionIdentifier overload"))
  }

  // Spark 4.2 widened `UnresolvedFunction.ignoreNulls` from `Boolean` to `Option[Boolean]`. Name
  // and (empty) parameter list are unchanged, and `getMethod` ignores the return type, so a single
  // lookup against the declared class covers every version.
  private lazy val ignoreNullsMethod: java.lang.reflect.Method =
    classOf[UnresolvedFunction].getMethod("ignoreNulls")

  def ignoreNulls(u: UnresolvedFunction): Boolean =
    toBoolean(invoke[AnyRef](ignoreNullsMethod, u))

  /**
   * Normalizes a reflectively read `ignoreNulls` value. Absent (`None`, Spark 4.2+) means "not
   * specified", which is `false` — the same reading Spark itself applies in
   * `FunctionResolution.resolveIgnoreNulls`.
   *
   * Any other shape is rejected rather than quietly treated as `false`: silently dropping an
   * `IGNORE NULLS` clause would return wrong query results instead of failing, which is the worst
   * way for a compat layer to break.
   */
  private[shims] def toBoolean(raw: Any): Boolean = raw match {
    case b: java.lang.Boolean => b.booleanValue()
    case None => false
    case Some(b: java.lang.Boolean) => b.booleanValue()
    case other =>
      throw new IllegalStateException(s"Unexpected UnresolvedFunction.ignoreNulls value: $other")
  }
}
