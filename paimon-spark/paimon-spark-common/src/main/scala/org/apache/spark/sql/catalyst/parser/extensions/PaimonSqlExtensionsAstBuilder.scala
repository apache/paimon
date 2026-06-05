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

package org.apache.spark.sql.catalyst.parser.extensions

import org.apache.paimon.spark.catalyst.plans.logical
import org.apache.paimon.spark.catalyst.plans.logical._
import org.apache.paimon.utils.TimeUtils

import org.antlr.v4.runtime._
import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.tree.{ParseTree, TerminalNode}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.parser.extensions.PaimonParserUtils.withOrigin
import org.apache.spark.sql.catalyst.parser.extensions.PaimonSqlExtensionsParser._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command.{CreateTableLikeCommand => SparkCreateTableLikeCommand}

import scala.collection.JavaConverters._
import scala.collection.mutable

/* This file is based on source code from the Iceberg Project (http://iceberg.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * The AST Builder provides an implementation of [[PaimonSqlExtensionsBaseVisitor]], which can be
 * extended to create a visitor which only needs to handle a subset of the available methods.
 *
 * <p>Most of the content of this class is referenced from Iceberg's IcebergSqlExtensionsAstBuilder.
 *
 * @param delegate
 *   The extension parser.
 */
class PaimonSqlExtensionsAstBuilder(delegate: ParserInterface)
  extends PaimonSqlExtensionsBaseVisitor[AnyRef]
  with Logging {

  /** Creates a single statement of extension statements. */
  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    visit(ctx.statement).asInstanceOf[LogicalPlan]
  }

  /** Creates a [[PaimonCallStatement]] for a stored procedure call. */
  override def visitCall(ctx: CallContext): PaimonCallStatement = withOrigin(ctx) {
    val name = ctx.multipartIdentifier.parts.asScala.map(_.getText).toSeq
    val args = ctx.callArgument.asScala.map(typedVisit[PaimonCallArgument]).toSeq
    logical.PaimonCallStatement(name, args)
  }

  /** Creates a positional argument in a stored procedure call. */
  override def visitPositionalArgument(ctx: PositionalArgumentContext): PaimonCallArgument =
    withOrigin(ctx) {
      val expression = typedVisit[Expression](ctx.expression)
      PaimonPositionalArgument(expression)
    }

  /** Creates a named argument in a stored procedure call. */
  override def visitNamedArgument(ctx: NamedArgumentContext): PaimonCallArgument = withOrigin(ctx) {
    val name = ctx.identifier.getText
    val expression = typedVisit[Expression](ctx.expression)
    PaimonNamedArgument(name, expression)
  }

  /** Creates a [[Expression]] in a positional and named argument. */
  override def visitExpression(ctx: ExpressionContext): Expression = {
    // reconstruct the SQL string and parse it using the main Spark parser
    // while we can avoid the logic to build Spark expressions, we still have to parse them
    // we cannot call ctx.getText directly since it will not render spaces correctly
    // that's why we need to recurse down the tree in reconstructSqlString
    val sqlString = reconstructSqlString(ctx)
    delegate.parseExpression(sqlString)
  }

  /** Returns a multi-part identifier as Seq[String]. */
  override def visitMultipartIdentifier(ctx: MultipartIdentifierContext): Seq[String] =
    withOrigin(ctx) {
      ctx.parts.asScala.map(_.getText).toSeq
    }

  /** Create a SHOW TAGS logical command. */
  override def visitShowTags(ctx: ShowTagsContext): ShowTagsCommand = withOrigin(ctx) {
    ShowTagsCommand(typedVisit[Seq[String]](ctx.multipartIdentifier))
  }

  /** Create a CREATE TABLE LIKE logical command. */
  override def visitCreateTableLike(ctx: CreateTableLikeContext): LogicalPlan = withOrigin(ctx) {
    sparkCreateTableLikeCommand(ctx).copy(
      targetTable = toTableIdentifier(typedVisit[Seq[String]](ctx.target)),
      sourceTable = toTableIdentifier(typedVisit[Seq[String]](ctx.source)))
  }

  /** Create a CREATE OR REPLACE TAG logical command. */
  override def visitCreateOrReplaceTag(ctx: CreateOrReplaceTagContext): CreateOrReplaceTagCommand =
    withOrigin(ctx) {
      val createTagClause = ctx.createReplaceTagClause()

      val tagName = createTagClause.identifier().getText
      val tagOptionsContext = Option(createTagClause.tagOptions())
      val snapshotId =
        tagOptionsContext
          .flatMap(tagOptions => Option(tagOptions.snapshotId()))
          .map(_.getText.toLong)
      val timeRetainCtx = tagOptionsContext.flatMap(tagOptions => Option(tagOptions.timeRetain()))
      val timeRetained = if (timeRetainCtx.nonEmpty) {
        val (number, timeUnit) =
          timeRetainCtx
            .map(retain => (retain.number().getText.toLong, retain.timeUnit().getText))
            .get
        Option(TimeUtils.parseDuration(number, timeUnit))
      } else {
        None
      }
      val tagOptions = TagOptions(
        snapshotId,
        timeRetained
      )

      val create = createTagClause.CREATE() != null
      val replace = createTagClause.REPLACE() != null
      val ifNotExists = createTagClause.EXISTS() != null

      CreateOrReplaceTagCommand(
        typedVisit[Seq[String]](ctx.multipartIdentifier),
        tagName,
        tagOptions,
        create,
        replace,
        ifNotExists)
    }

  /** Create a DELETE TAG logical command. */
  override def visitDeleteTag(ctx: DeleteTagContext): DeleteTagCommand = withOrigin(ctx) {
    DeleteTagCommand(
      typedVisit[Seq[String]](ctx.multipartIdentifier),
      ctx.identifier().getText,
      ctx.EXISTS() != null)
  }

  /** Create a RENAME TAG logical command. */
  override def visitRenameTag(ctx: RenameTagContext): RenameTagCommand = withOrigin(ctx) {
    RenameTagCommand(
      typedVisit[Seq[String]](ctx.multipartIdentifier),
      ctx.identifier(0).getText,
      ctx.identifier(1).getText)
  }

  /** Create a COPY INTO TABLE (import) logical command. */
  override def visitCopyIntoTable(ctx: CopyIntoTableContext): logical.CopyIntoTableCommand =
    withOrigin(ctx) {
      val table = typedVisit[Seq[String]](ctx.multipartIdentifier)
      val columns = Option(ctx.columnList()).map(_.identifier().asScala.map(_.getText).toSeq)
      val sourcePath = unquoteString(ctx.sourcePath.getText)
      val fileFormat = buildFileFormat(ctx.fileFormatClause())
      val pattern = Option(ctx.patternClause()).map(p => unquoteString(p.STRING().getText))
      val force = Option(ctx.forceClause()).exists(_.booleanValue().TRUE() != null)
      val onError = Option(ctx.onErrorClause())
        .map {
          clause =>
            if (clause.CONTINUE() != null) OnErrorMode.Continue
            else if (clause.SKIP_FILE() != null) OnErrorMode.SkipFile
            else OnErrorMode.AbortStatement
        }
        .getOrElse(OnErrorMode.AbortStatement)
      logical.CopyIntoTableCommand(table, columns, sourcePath, fileFormat, pattern, force, onError)
    }

  /** Create a COPY INTO LOCATION (export) logical command. */
  override def visitCopyIntoLocation(
      ctx: CopyIntoLocationContext): logical.CopyIntoLocationCommand = withOrigin(ctx) {
    val targetPath = unquoteString(ctx.targetPath.getText)
    val table = typedVisit[Seq[String]](ctx.multipartIdentifier)
    val fileFormat = buildFileFormat(ctx.fileFormatClause())
    val overwrite = Option(ctx.overwriteClause()).exists(_.booleanValue().TRUE() != null)
    logical.CopyIntoLocationCommand(targetPath, table, fileFormat, overwrite)
  }

  private def buildFileFormat(ctx: FileFormatClauseContext): CopyFileFormat = {
    val opts = ctx.fileFormatOption().asScala.toSeq
    val seen = mutable.Set[String]()
    val optionsBuilder = mutable.LinkedHashMap[String, String]()

    opts.foreach {
      opt =>
        val key = opt.key.getText.toUpperCase
        if (!seen.add(key)) {
          throw new IllegalArgumentException(s"Duplicate FILE_FORMAT option: $key")
        }
        val value = extractFormatValue(opt.fileFormatValue())
        optionsBuilder(key) = value
    }

    val typeValue = optionsBuilder.remove("TYPE")
    if (typeValue.isEmpty) {
      throw new IllegalArgumentException("FILE_FORMAT must include TYPE")
    }

    val formatType = CopyFileFormat.parseFormatType(typeValue.get)

    CopyFileFormat(formatType, optionsBuilder.toMap)
  }

  private def extractFormatValue(ctx: FileFormatValueContext): String = {
    ctx match {
      case c: StringFormatValueContext =>
        unquoteString(c.STRING().getText)
      case c: IdentFormatValueContext =>
        c.identifier().getText
      case c: BoolFormatValueContext =>
        if (c.booleanValue().TRUE() != null) "TRUE" else "FALSE"
      case c: IntFormatValueContext =>
        c.INTEGER_VALUE().getText
      case c: ListFormatValueContext =>
        c.STRING()
          .asScala
          .map(s => unquoteString(s.getText))
          .mkString(CopyFileFormat.LIST_SEPARATOR)
    }
  }

  private def unquoteString(s: String): String = {
    if (s == null || s.length < 2) return s
    val first = s.charAt(0)
    if ((first == '\'' || first == '"') && s.charAt(s.length - 1) == first) {
      val inner = s.substring(1, s.length - 1)
      val sb = new StringBuilder
      var i = 0
      while (i < inner.length) {
        val c = inner.charAt(i)
        if (c == '\\' && i + 1 < inner.length) {
          inner.charAt(i + 1) match {
            case 'n' => sb.append('\n'); i += 2
            case 't' => sb.append('\t'); i += 2
            case 'r' => sb.append('\r'); i += 2
            case '\\' => sb.append('\\'); i += 2
            case q if q == first => sb.append(q); i += 2
            case other => sb.append('\\'); sb.append(other); i += 2
          }
        } else if (c == first && i + 1 < inner.length && inner.charAt(i + 1) == first) {
          sb.append(first); i += 2
        } else {
          sb.append(c); i += 1
        }
      }
      sb.toString()
    } else {
      s
    }
  }

  private def toBuffer[T](list: java.util.List[T]) = list.asScala

  private def toSeq[T](list: java.util.List[T]) = toBuffer(list)

  private def toTableIdentifier(identifier: Seq[String]): TableIdentifier = {
    identifier match {
      case Seq(table) =>
        TableIdentifier(table)
      case Seq(database, table) =>
        TableIdentifier(table, Some(database))
      case parts =>
        TableIdentifier(
          parts.last,
          Some(parts.slice(1, parts.length - 1).mkString(".")),
          Some(parts.head))
    }
  }

  private def sparkCreateTableLikeCommand(
      ctx: CreateTableLikeContext): SparkCreateTableLikeCommand = {
    delegate.parsePlan(createSparkCreateTableLikeSql(ctx)) match {
      case command: SparkCreateTableLikeCommand => command
      case plan =>
        throw new UnsupportedOperationException(
          s"Expected Spark CREATE TABLE LIKE command, but got ${plan.nodeName}.")
    }
  }

  private def createSparkCreateTableLikeSql(ctx: CreateTableLikeContext): String = {
    val stream = ctx.getStart.getInputStream
    val baseStart = ctx.getStart.getStartIndex
    val baseStop = ctx.getStop.getStopIndex
    val targetStart = ctx.target.getStart.getStartIndex
    val targetStop = ctx.target.getStop.getStopIndex
    val sourceStart = ctx.source.getStart.getStartIndex
    val sourceStop = ctx.source.getStop.getStopIndex

    val prefix = stream.getText(Interval.of(baseStart, targetStart - 1))
    val middle = stream.getText(Interval.of(targetStop + 1, sourceStart - 1))
    val suffix = if (sourceStop < baseStop) {
      stream.getText(Interval.of(sourceStop + 1, baseStop))
    } else {
      ""
    }

    prefix + "__paimon_create_like_target" + middle + "__paimon_create_like_source" + suffix
  }

  private def reconstructSqlString(ctx: ParserRuleContext): String = {
    toBuffer(ctx.children)
      .map {
        case c: ParserRuleContext => reconstructSqlString(c)
        case t: TerminalNode => t.getText
      }
      .mkString(" ")
  }

  private def typedVisit[T](ctx: ParseTree): T =
    ctx.accept(this).asInstanceOf[T]
}

/* Partially copied from Apache Spark's Parser to avoid dependency on Spark Internals */
object PaimonParserUtils {

  private[sql] def withOrigin[T](ctx: ParserRuleContext)(f: => T): T = {
    val current = CurrentOrigin.get
    CurrentOrigin.set(position(ctx.getStart))
    try {
      f
    } finally {
      CurrentOrigin.set(current)
    }
  }

  private[sql] def position(token: Token): Origin = {
    val opt = Option(token)
    Origin(opt.map(_.getLine), opt.map(_.getCharPositionInLine))
  }

  /** Gets the command which created the token. */
  private[sql] def command(ctx: ParserRuleContext): String = {
    val stream = ctx.getStart.getInputStream
    stream.getText(Interval.of(0, stream.size() - 1))
  }
}

case class Origin(
    line: Option[Int] = None,
    startPosition: Option[Int] = None,
    startIndex: Option[Int] = None,
    stopIndex: Option[Int] = None,
    sqlText: Option[String] = None,
    objectType: Option[String] = None,
    objectName: Option[String] = None) {}

object CurrentOrigin {
  private val value = new ThreadLocal[Origin]() {
    override def initialValue: Origin = Origin()
  }

  def get: Origin = value.get()
  def set(o: Origin): Unit = value.set(o)
  def reset(): Unit = value.set(Origin())

  def withOrigin[A](o: Origin)(f: => A): A = {
    // remember the previous one so it can be reset to this
    // way withOrigin can be recursive
    val previous = get
    set(o)
    val ret =
      try f
      finally { set(previous) }
    ret
  }
}
/* Apache Spark copy end */
