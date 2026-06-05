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

import org.apache.paimon.spark.SparkProcedures

import org.antlr.v4.runtime._
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.{Interval, ParseCancellationException}
import org.antlr.v4.runtime.tree.TerminalNodeImpl
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, PaimonSparkSession, SparkSession}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserInterface}
import org.apache.spark.sql.catalyst.parser.extensions.PaimonSqlExtensionsParser.{NonReservedContext, QuotedIdentifierContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.VariableSubstitution
import org.apache.spark.sql.types.{DataType, StructType}

import java.util.Locale

import scala.collection.JavaConverters._

/* This file is based on source code from the Iceberg Project (http://iceberg.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

abstract class AbstractPaimonSparkSqlExtensionsParser(val delegate: ParserInterface)
  extends Logging {

  private lazy val substitutor = new VariableSubstitution()
  private lazy val astBuilder = new PaimonSqlExtensionsAstBuilder(delegate)
  private val nonReservedIdentifierTokenTypes = Set(
    PaimonSqlExtensionsParser.ALTER,
    PaimonSqlExtensionsParser.AS,
    PaimonSqlExtensionsParser.CALL,
    PaimonSqlExtensionsParser.CREATE,
    PaimonSqlExtensionsParser.DAYS,
    PaimonSqlExtensionsParser.DELETE,
    PaimonSqlExtensionsParser.EXISTS,
    PaimonSqlExtensionsParser.HOURS,
    PaimonSqlExtensionsParser.IF,
    PaimonSqlExtensionsParser.LIKE,
    PaimonSqlExtensionsParser.NOT,
    PaimonSqlExtensionsParser.OF,
    PaimonSqlExtensionsParser.OR,
    PaimonSqlExtensionsParser.TABLE,
    PaimonSqlExtensionsParser.REPLACE,
    PaimonSqlExtensionsParser.RETAIN,
    PaimonSqlExtensionsParser.VERSION,
    PaimonSqlExtensionsParser.TAG,
    PaimonSqlExtensionsParser.TRUE,
    PaimonSqlExtensionsParser.FALSE,
    PaimonSqlExtensionsParser.MAP,
    PaimonSqlExtensionsParser.COPY,
    PaimonSqlExtensionsParser.INTO,
    PaimonSqlExtensionsParser.FROM,
    PaimonSqlExtensionsParser.FILE_FORMAT,
    PaimonSqlExtensionsParser.PATTERN,
    PaimonSqlExtensionsParser.FORCE,
    PaimonSqlExtensionsParser.ON_ERROR,
    PaimonSqlExtensionsParser.ABORT_STATEMENT,
    PaimonSqlExtensionsParser.CONTINUE,
    PaimonSqlExtensionsParser.SKIP_FILE,
    PaimonSqlExtensionsParser.OVERWRITE,
    PaimonSqlExtensionsParser.CSV
  )

  /** Parses a string to a LogicalPlan. */
  def parsePlan(sqlText: String): LogicalPlan = {
    val sqlTextAfterSubstitution = substitutor.substitute(sqlText)
    if (isPaimonCommand(sqlTextAfterSubstitution)) {
      parse(sqlTextAfterSubstitution)(parser => astBuilder.visit(parser.singleStatement()))
        .asInstanceOf[LogicalPlan]
    } else {
      var plan =
        try {
          delegate.parsePlan(sqlText)
        } catch {
          case _: ParseException if maybeCatalogCreateTableLike(sqlTextAfterSubstitution) =>
            parse(sqlTextAfterSubstitution)(parser => astBuilder.visit(parser.singleStatement()))
              .asInstanceOf[LogicalPlan]
        }
      val sparkSession = PaimonSparkSession.active
      parserRules(sparkSession).foreach(
        rule => {
          plan = rule.apply(plan)
        })
      plan
    }
  }

  private def parserRules(sparkSession: SparkSession): Seq[Rule[LogicalPlan]] = {
    Seq(
      RewritePaimonViewCommands(sparkSession),
      RewritePaimonFunctionCommands(sparkSession),
      RewriteCreateTableLikeCommand(sparkSession),
      RewriteSparkDDLCommands(sparkSession)
    )
  }

  /** Parses a string to an Expression. */
  def parseExpression(sqlText: String): Expression =
    delegate.parseExpression(sqlText)

  /** Parses a string to a TableIdentifier. */
  def parseTableIdentifier(sqlText: String): TableIdentifier =
    delegate.parseTableIdentifier(sqlText)

  /** Parses a string to a FunctionIdentifier. */
  def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    delegate.parseFunctionIdentifier(sqlText)

  /**
   * Creates StructType for a given SQL string, which is a comma separated list of field definitions
   * which will preserve the correct Hive metadata.
   */
  def parseTableSchema(sqlText: String): StructType =
    delegate.parseTableSchema(sqlText)

  /** Parses a string to a DataType. */
  def parseDataType(sqlText: String): DataType =
    delegate.parseDataType(sqlText)

  /** Parses a string to a multi-part identifier. */
  def parseMultipartIdentifier(sqlText: String): Seq[String] =
    delegate.parseMultipartIdentifier(sqlText)

  /** Returns whether SQL text is command. */
  private def isPaimonCommand(sqlText: String): Boolean = {
    val normalized = sqlText
      .toLowerCase(Locale.ROOT)
      .trim()
      .replaceAll("--.*?\\n", " ")
      .replaceAll("\\s+", " ")
      .replaceAll("/\\*.*?\\*/", " ")
      .replaceAll("`", "")
      .trim()
    isPaimonProcedure(normalized) || isTagRefDdl(normalized) || isCopyInto(normalized)
  }

  // All builtin paimon procedures are under the 'sys' namespace
  private def isPaimonProcedure(normalized: String): Boolean = {
    normalized.startsWith("call") &&
    SparkProcedures.names().asScala.map("sys." + _).exists(normalized.contains)
  }

  private def isTagRefDdl(normalized: String): Boolean = {
    normalized.startsWith("show tags") ||
    (normalized.startsWith("alter table") &&
      (normalized.contains("create tag") ||
        normalized.contains("replace tag") ||
        normalized.contains("rename tag") ||
        normalized.contains("delete tag")))
  }

  private def isCopyInto(normalized: String): Boolean = {
    normalized.startsWith("copy into")
  }

  /**
   * Cheap token-level check for `CREATE TABLE [IF NOT EXISTS] x.y[.z] LIKE ...` shape. Used as a
   * gate for the Paimon parser fallback when the delegate parser rejects a catalog-qualified CREATE
   * TABLE LIKE statement.
   */
  private def maybeCatalogCreateTableLike(sqlText: String): Boolean = {
    if (org.apache.spark.SPARK_VERSION < "3.4") {
      return false
    }
    if (!startsWithCreateTable(sqlText)) {
      return false
    }

    tokenStream(sqlText) match {
      case Some(tokens) => maybeCreateTableLike(tokens)
      case None => false
    }
  }

  private def tokenStream(sqlText: String): Option[CommonTokenStream] = {
    try {
      val lexer = new PaimonSqlExtensionsLexer(
        new UpperCaseCharStream(CharStreams.fromString(sqlText)))
      lexer.removeErrorListeners()
      lexer.addErrorListener(PaimonParseErrorListener)

      val tokens = new CommonTokenStream(lexer)
      tokens.fill()
      Some(tokens)
    } catch {
      case _: PaimonParseException => None
    }
  }

  private def maybeCreateTableLike(tokenStream: CommonTokenStream): Boolean = {
    val tokens = tokenStream.getTokens.asScala
      .filter(token => token.getChannel == Token.DEFAULT_CHANNEL)
      .filterNot(token => token.getType == Token.EOF)

    if (tokens.length < 5) return false
    if (tokens(0).getType != PaimonSqlExtensionsParser.CREATE) return false
    if (tokens(1).getType != PaimonSqlExtensionsParser.TABLE) return false

    var idx = 2
    if (
      idx + 2 < tokens.length &&
      tokens(idx).getType == PaimonSqlExtensionsParser.IF &&
      tokens(idx + 1).getType == PaimonSqlExtensionsParser.NOT &&
      tokens(idx + 2).getType == PaimonSqlExtensionsParser.EXISTS
    ) {
      idx += 3
    }

    if (idx >= tokens.length || !isIdentifierToken(tokens(idx))) return false
    idx += 1

    while (
      idx + 1 < tokens.length &&
      tokens(idx).getText == "." &&
      isIdentifierToken(tokens(idx + 1))
    ) {
      idx += 2
    }

    idx < tokens.length && tokens(idx).getType == PaimonSqlExtensionsParser.LIKE
  }

  private def isIdentifierToken(token: Token): Boolean = {
    token.getType == PaimonSqlExtensionsParser.IDENTIFIER ||
    token.getType == PaimonSqlExtensionsParser.BACKQUOTED_IDENTIFIER ||
    nonReservedIdentifierTokenTypes.contains(token.getType)
  }

  private def startsWithCreateTable(sqlText: String): Boolean = {
    val createIndex = skipWhitespaceAndComments(sqlText, 0)
    if (!matchesWord(sqlText, createIndex, "create")) {
      return false
    }

    val tableIndex = skipWhitespaceAndComments(sqlText, createIndex + "create".length)
    matchesWord(sqlText, tableIndex, "table")
  }

  private def skipWhitespaceAndComments(sqlText: String, start: Int): Int = {
    var index = start
    var continue = true

    while (continue) {
      while (index < sqlText.length && sqlText.charAt(index).isWhitespace) {
        index += 1
      }

      if (
        index + 1 < sqlText.length &&
        sqlText.charAt(index) == '-' &&
        sqlText.charAt(index + 1) == '-'
      ) {
        index += 2
        while (
          index < sqlText.length &&
          sqlText.charAt(index) != '\n' &&
          sqlText.charAt(index) != '\r'
        ) {
          index += 1
        }
      } else if (
        index + 1 < sqlText.length &&
        sqlText.charAt(index) == '/' &&
        sqlText.charAt(index + 1) == '*'
      ) {
        val close = sqlText.indexOf("*/", index + 2)
        index = if (close >= 0) close + 2 else sqlText.length
      } else {
        continue = false
      }
    }

    index
  }

  private def matchesWord(sqlText: String, index: Int, word: String): Boolean = {
    index + word.length <= sqlText.length &&
    sqlText.regionMatches(true, index, word, 0, word.length) &&
    (index + word.length == sqlText.length ||
      !isIdentifierPart(sqlText.charAt(index + word.length)))
  }

  private def isIdentifierPart(char: Char): Boolean = {
    char.isLetterOrDigit || char == '_'
  }

  protected def parse[T](command: String)(toResult: PaimonSqlExtensionsParser => T): T = {
    val lexer = new PaimonSqlExtensionsLexer(
      new UpperCaseCharStream(CharStreams.fromString(command)))
    lexer.removeErrorListeners()
    lexer.addErrorListener(PaimonParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new PaimonSqlExtensionsParser(tokenStream)
    parser.addParseListener(PaimonSqlExtensionsPostProcessor)
    parser.removeErrorListeners()
    parser.addErrorListener(PaimonParseErrorListener)

    try {
      try {
        parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
        toResult(parser)
      } catch {
        case _: ParseCancellationException =>
          tokenStream.seek(0)
          parser.reset()
          parser.getInterpreter.setPredictionMode(PredictionMode.LL)
          toResult(parser)
      }
    } catch {
      case e: PaimonParseException if e.command.isDefined =>
        throw e
      case e: PaimonParseException =>
        throw e.withCommand(command)
      case e: AnalysisException =>
        val position = Origin(e.line, e.startPosition)
        throw new PaimonParseException(Option(command), e.message, position, position)
    }
  }

  def parseQuery(sqlText: String): LogicalPlan =
    parsePlan(sqlText)
}

/* Copied from Apache Spark's to avoid dependency on Spark Internals */
class UpperCaseCharStream(wrapped: CodePointCharStream) extends CharStream {
  override def consume(): Unit = wrapped.consume()
  override def getSourceName: String = wrapped.getSourceName
  override def index(): Int = wrapped.index
  override def mark(): Int = wrapped.mark
  override def release(marker: Int): Unit = wrapped.release(marker)
  override def seek(where: Int): Unit = wrapped.seek(where)
  override def size(): Int = wrapped.size

  override def getText(interval: Interval): String = wrapped.getText(interval)

  // scalastyle:off
  override def LA(i: Int): Int = {
    val la = wrapped.LA(i)
    if (la == 0 || la == IntStream.EOF) la
    else Character.toUpperCase(la)
  }
  // scalastyle:on
}

/** The post-processor validates & cleans-up the parse tree during the parse process. */
case object PaimonSqlExtensionsPostProcessor extends PaimonSqlExtensionsBaseListener {

  /** Removes the back ticks from an Identifier. */
  override def exitQuotedIdentifier(ctx: QuotedIdentifierContext): Unit = {
    replaceTokenByIdentifier(ctx, 1) {
      token =>
        // Remove the double back ticks in the string.
        token.setText(token.getText.replace("``", "`"))
        token
    }
  }

  /** Treats non-reserved keywords as Identifiers. */
  override def exitNonReserved(ctx: NonReservedContext): Unit = {
    replaceTokenByIdentifier(ctx, 0)(identity)
  }

  private def replaceTokenByIdentifier(ctx: ParserRuleContext, stripMargins: Int)(
      f: CommonToken => CommonToken = identity): Unit = {
    val parent = ctx.getParent
    parent.removeLastChild()
    val token = ctx.getChild(0).getPayload.asInstanceOf[Token]
    val newToken = new CommonToken(
      new org.antlr.v4.runtime.misc.Pair(token.getTokenSource, token.getInputStream),
      PaimonSqlExtensionsParser.IDENTIFIER,
      token.getChannel,
      token.getStartIndex + stripMargins,
      token.getStopIndex - stripMargins
    )
    parent.addChild(new TerminalNodeImpl(f(newToken)))
  }
}

/* Partially copied from Apache Spark's Parser to avoid dependency on Spark Internals */
case object PaimonParseErrorListener extends BaseErrorListener {
  override def syntaxError(
      recognizer: Recognizer[_, _],
      offendingSymbol: scala.Any,
      line: Int,
      charPositionInLine: Int,
      msg: String,
      e: RecognitionException): Unit = {
    val (start, stop) = offendingSymbol match {
      case token: CommonToken =>
        val start = Origin(Some(line), Some(token.getCharPositionInLine))
        val length = token.getStopIndex - token.getStartIndex + 1
        val stop = Origin(Some(line), Some(token.getCharPositionInLine + length))
        (start, stop)
      case _ =>
        val start = Origin(Some(line), Some(charPositionInLine))
        (start, start)
    }
    throw new PaimonParseException(None, msg, start, stop)
  }
}

/**
 * Copied from Apache Spark [[ParseException]], it contains fields and an extended error message
 * that make reporting and diagnosing errors easier.
 */
class PaimonParseException(
    val command: Option[String],
    message: String,
    start: Origin,
    stop: Origin)
  extends Exception {

  override def getMessage: String = {
    val builder = new StringBuilder
    builder ++= "\n" ++= message
    start match {
      case Origin(Some(l), Some(p), Some(_), Some(_), Some(_), Some(_), Some(_)) =>
        builder ++= s"(line $l, pos $p)\n"
        command.foreach {
          cmd =>
            val (above, below) = cmd.split("\n").splitAt(l)
            builder ++= "\n== SQL ==\n"
            above.foreach(builder ++= _ += '\n')
            builder ++= (0 until p).map(_ => "-").mkString("") ++= "^^^\n"
            below.foreach(builder ++= _ += '\n')
        }
      case _ =>
        command.foreach(cmd => builder ++= "\n== SQL ==\n" ++= cmd)
    }
    builder.toString
  }

  def withCommand(cmd: String): PaimonParseException =
    new PaimonParseException(Option(cmd), message, start, stop)
}
