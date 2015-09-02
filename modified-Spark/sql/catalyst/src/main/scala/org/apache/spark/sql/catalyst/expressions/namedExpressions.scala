/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.trees.LeafNode
import org.apache.spark.sql.types._

object NamedExpression {
  private val curId = new java.util.concurrent.atomic.AtomicLong()
  def newExprId: ExprId = ExprId(curId.getAndIncrement())
  def unapply(expr: NamedExpression): Option[(String, DataType)] = Some(expr.name, expr.dataType)
}

/**
 * A globally unique (within this JVM) id for a given named expression.
 * Used to identify which attribute output by a relation is being
 * referenced in a subsequent computation.
 */
case class ExprId(id: Long)

abstract class NamedExpression extends Expression {
  self: Product =>

  def name: String
  def exprId: ExprId

  /**
   * Returns a dot separated fully qualified name for this attribute.  Given that there can be
   * multiple qualifiers, it is possible that there are other possible way to refer to this
   * attribute.
   */
  def qualifiedName: String = (qualifiers.headOption.toSeq :+ name).mkString(".")

  /**
   * All possible qualifiers for the expression.
   *
   * For now, since we do not allow using original table name to qualify a column name once the
   * table is aliased, this can only be:
   *
   * 1. Empty Seq: when an attribute doesn't have a qualifier,
   *    e.g. top level attributes aliased in the SELECT clause, or column from a LocalRelation.
   * 2. Single element: either the table name or the alias name of the table.
   */
  def qualifiers: Seq[String]

  def toAttribute: Attribute

  /** Returns the metadata when an expression is a reference to another expression with metadata. */
  def metadata: Metadata = Metadata.empty

  protected def typeSuffix =
    if (resolved) {
      dataType match {
        case LongType => "L"
        case _ => ""
      }
    } else {
      ""
    }
}

abstract class Attribute extends NamedExpression {
  self: Product =>

  override def references: AttributeSet = AttributeSet(this)

  def withNullability(newNullability: Boolean): Attribute
  def withQualifiers(newQualifiers: Seq[String]): Attribute
  def withName(newName: String): Attribute

  def toAttribute: Attribute = this
  def newInstance(): Attribute

}

/**
 * Used to assign a new name to a computation.
 * For example the SQL expression "1 + 1 AS a" could be represented as follows:
 *  Alias(Add(Literal(1), Literal(1)), "a")()
 *
 * Note that exprId and qualifiers are in a separate parameter list because
 * we only pattern match on child and name.
 *
 * @param child the computation being performed
 * @param name the name to be associated with the result of computing [[child]].
 * @param exprId A globally unique id used to check if an [[AttributeReference]] refers to this
 *               alias. Auto-assigned if left blank.
 */
case class Alias(child: Expression, name: String)
    (val exprId: ExprId = NamedExpression.newExprId, val qualifiers: Seq[String] = Nil)
  extends NamedExpression with trees.UnaryNode[Expression] {

  override type EvaluatedType = Any

  override def eval(input: Row): Any = child.eval(input)

  override def dataType: DataType = child.dataType
  override def nullable: Boolean = child.nullable
  override def metadata: Metadata = {
    child match {
      case named: NamedExpression => named.metadata
      case _ => Metadata.empty
    }
  }

  override def toAttribute: Attribute = {
    if (resolved) {
      AttributeReference(name, child.dataType, child.nullable, metadata)(exprId, qualifiers)
    } else {
      UnresolvedAttribute(name)
    }
  }

  override def toString: String = s"$child AS $name#${exprId.id}$typeSuffix"

  override protected final def otherCopyArgs: Seq[AnyRef] = exprId :: qualifiers :: Nil

  override def equals(other: Any): Boolean = other match {
    case a: Alias =>
      name == a.name && exprId == a.exprId && child == a.child && qualifiers == a.qualifiers
    case _ => false
  }
}

/**
 * A reference to an attribute produced by another operator in the tree.
 *
 * @param name The name of this attribute, should only be used during analysis or for debugging.
 * @param dataType The [[DataType]] of this attribute.
 * @param nullable True if null is a valid value for this attribute.
 * @param metadata The metadata of this attribute.
 * @param exprId A globally unique id used to check if different AttributeReferences refer to the
 *               same attribute.
 * @param qualifiers a list of strings that can be used to referred to this attribute in a fully
 *                   qualified way. Consider the examples tableName.name, subQueryAlias.name.
 *                   tableName and subQueryAlias are possible qualifiers.
 */
case class AttributeReference(
    name: String,
    dataType: DataType,
    nullable: Boolean = true,
    override val metadata: Metadata = Metadata.empty)(
    val exprId: ExprId = NamedExpression.newExprId,
    val qualifiers: Seq[String] = Nil) extends Attribute with trees.LeafNode[Expression] {

  override def equals(other: Any): Boolean = other match {
    case ar: AttributeReference => name == ar.name && exprId == ar.exprId && dataType == ar.dataType
    case _ => false
  }

  override def hashCode: Int = {
    // See http://stackoverflow.com/questions/113511/hash-code-implementation
    var h = 17
    h = h * 37 + exprId.hashCode()
    h = h * 37 + dataType.hashCode()
    h = h * 37 + metadata.hashCode()
    h
  }

  override def newInstance(): AttributeReference =
    AttributeReference(name, dataType, nullable, metadata)(qualifiers = qualifiers)

  /**
   * Returns a copy of this [[AttributeReference]] with changed nullability.
   */
  override def withNullability(newNullability: Boolean): AttributeReference = {
    if (nullable == newNullability) {
      this
    } else {
      AttributeReference(name, dataType, newNullability, metadata)(exprId, qualifiers)
    }
  }

  override def withName(newName: String): AttributeReference = {
    if (name == newName) {
      this
    } else {
      AttributeReference(newName, dataType, nullable)(exprId, qualifiers)
    }
  }

  /**
   * Returns a copy of this [[AttributeReference]] with new qualifiers.
   */
  override def withQualifiers(newQualifiers: Seq[String]): AttributeReference = {
    if (newQualifiers.toSet == qualifiers.toSet) {
      this
    } else {
      AttributeReference(name, dataType, nullable, metadata)(exprId, newQualifiers)
    }
  }

  // Unresolved attributes are transient at compile time and don't get evaluated during execution.
  override def eval(input: Row = null): EvaluatedType =
    throw new TreeNodeException(this, s"No function to evaluate expression. type: ${this.nodeName}")

  override def toString: String = s"$name#${exprId.id}$typeSuffix"
}

/**
 * A place holder used when printing expressions without debugging information such as the
 * expression id or the unresolved indicator.
 */
case class PrettyAttribute(name: String) extends Attribute with trees.LeafNode[Expression] {
  type EvaluatedType = Any

  override def toString: String = name

  override def withNullability(newNullability: Boolean): Attribute =
    throw new UnsupportedOperationException
  override def newInstance(): Attribute = throw new UnsupportedOperationException
  override def withQualifiers(newQualifiers: Seq[String]): Attribute =
    throw new UnsupportedOperationException
  override def withName(newName: String): Attribute = throw new UnsupportedOperationException
  override def qualifiers: Seq[String] = throw new UnsupportedOperationException
  override def exprId: ExprId = throw new UnsupportedOperationException
  override def eval(input: Row): EvaluatedType = throw new UnsupportedOperationException
  override def nullable: Boolean = throw new UnsupportedOperationException
  override def dataType: DataType = NullType
}

object VirtualColumn {
  val groupingIdName: String = "grouping__id"
  def newGroupingId: AttributeReference = AttributeReference(groupingIdName, IntegerType, false)()
}
