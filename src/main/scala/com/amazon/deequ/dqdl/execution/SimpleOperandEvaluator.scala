/**
 * Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 * http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package com.amazon.deequ.dqdl.execution

import com.amazon.deequ.dqdl.execution.ResultType.ResultType
import org.apache.log4j.Logger
import software.amazon.glue.dqdl.model.DQRule
import software.amazon.glue.dqdl.model.condition.number.{AtomicNumberOperand, BinaryExpressionOperand, NumericOperand, OperandEvaluator}

import java.lang
import scala.util.{Failure, Success, Try}

object ResultType extends Enumeration {
  type ResultType = Value
  val Double, ListDouble = Value
}

/**
 * Represents an intermediate result in operand graph evaluation.
 *
 */
final case class IntermediateResult(value: Object, valueType: ResultType) {
  /**
   * Resolves final result of function evaluation to a scalar.
   */
  def result(): Either[String, Double] = valueType match {

    case ResultType.Double =>
      Right(value.asInstanceOf[Double])
    case ResultType.ListDouble => value.asInstanceOf[List[Double]] match {
      case list if list.size == 1 =>
        Right(list.head)
      case list if list.isEmpty =>
        Left("Rule threshold results in empty list, and a single value is expected.")
      case list =>
        Left(s"Rule threshold results in list, and a single value is expected. Use aggregation functions to produce a" +
          s" single value. Valid example: sum(last(10)), avg(last(10))." +
          s"${list.size}")
    }
    case _ =>
      IntermediateResult.logError("Internal error: could not resolve final result from operand evaluation.")
      Left("Internal error")
  }

  /**
   * Overriding equals and hashcode because class has Object field - default implementation of case class eq/hashcode
   * will include object reference as hashcode for this field rather than resolved value.
   */
  override def equals(obj: Any): Boolean = {
    val tol = 0.0000001D
    obj match {
      case i: IntermediateResult if i.valueType == valueType && valueType.equals(ResultType.Double) =>
        (i.value.asInstanceOf[Double] - value.asInstanceOf[Double]).abs < tol
      case i: IntermediateResult if i.valueType == valueType && valueType.equals(ResultType.ListDouble) =>
        i.value.asInstanceOf[List[Double]] == value.asInstanceOf[List[Double]]
      case _ => false
    }
  }

  override def hashCode(): Int = valueType match {
    case v if v.equals(ResultType.Double) =>
      31 * value.asInstanceOf[Double].##
    case v if v.equals(ResultType.ListDouble) =>
      31 * value.asInstanceOf[List[Double]].hashCode()
    case _ =>
      IntermediateResult.logError("Internal error: unsupported result type.")
      throw new NotImplementedError("Internal error")
  }
}

object IntermediateResult {
  val log: Logger = Logger.getLogger(getClass.getName)

  // provide this method from companion object to ensure intermediate results remain serializable
  private def logError(s: String): Unit = {
    log.error(s)
  }

  def apply(value: Double): IntermediateResult = {
    new IntermediateResult(value.asInstanceOf[Object], ResultType.Double)
  }

  def apply(value: List[Double]): IntermediateResult = {
    new IntermediateResult(value.asInstanceOf[Object], ResultType.ListDouble)
  }
}

object SimpleOperandEvaluator extends OperandEvaluator {
  override def evaluate(dqRule: DQRule, numericOperand: NumericOperand): lang.Double =
    evaluateInner(dqRule, numericOperand).result() match {
      case Left(l) => throw new IllegalArgumentException(l)
      case Right(result) => result
    }

  private def evaluateInner(rule: DQRule, operand: NumericOperand): IntermediateResult = {
    val result = operand match {
      case op: AtomicNumberOperand => evaluateAtomicNumber(op)
      case op: BinaryExpressionOperand => evaluateBinaryExpression(rule, op)
      case op => Left(new IllegalArgumentException(s"Unknown operand type: ${op.getClass.toString}"))
    }
    result match {
      case Left(e) => throw e
      case Right(result) => result
    }
  }

  private def evaluateAtomicNumber(operand: AtomicNumberOperand)
  : Either[Throwable, IntermediateResult] = {
    Try {
      operand.toString.toDouble
    } match {
      case Success(d) => Right(IntermediateResult(d))
      case Failure(_) =>
        Left(new IllegalArgumentException(f"Cannot parse atomic operand ${operand.toString} to double value"))
    }
  }

  private def evaluateBinaryExpression(rule: DQRule, operand: BinaryExpressionOperand)
  : Either[Throwable, IntermediateResult] = {
    val opsAsDoubles = for {
      op1 <- evaluateInner(rule, operand.getOperand1).result()
      op2 <- evaluateInner(rule, operand.getOperand2).result()
    } yield (op1, op2)
    opsAsDoubles.fold(
      _ => Left(new IllegalArgumentException("Binary expression operands must resolve to a single number")),
      ops => operand.getOperator match {
        case "+" => Right(IntermediateResult(ops._1 + ops._2))
        case "-" => Right(IntermediateResult(ops._1 - ops._2))
        case "/" => Right(IntermediateResult(ops._1 / ops._2))
        case "*" => Right(IntermediateResult(ops._1 * ops._2))
        case _ => Left(
          new IllegalArgumentException(s"Unknown operator in binary expression: ${operand.getOperator}"))
      })
  }

}
