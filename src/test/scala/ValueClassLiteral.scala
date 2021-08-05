package cchantep.sandbox

import frameless.{ TypedEncoder, TypedColumn }

import scala.language.reflectiveCalls
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  Literal,
  NonSQLExpression
}
import org.apache.spark.sql.types.DataType

// TODO: Macro
case class ValueClassLiteral[T <: AnyVal { def value: Any }] private[sandbox] (
    obj: T,
    encoder: TypedEncoder[T])
    extends Expression
    with NonSQLExpression {
  override def nullable: Boolean = encoder.nullable
  override def toString: String = s"FLit($obj)"

  def eval(input: InternalRow): Any = {
    val ctx = new CodegenContext()
    val eval = genCode(ctx)

    val codeBody = s"""
      public scala.Function1<InternalRow, Object> generate(Object[] references) {
        return new ValueClassLiteralEvalImpl(references);
      }

      class ValueClassLiteralEvalImpl extends scala.runtime.AbstractFunction1<InternalRow, Object> {
        private final Object[] references;
        ${ctx.declareMutableStates()}
        ${ctx.declareAddedFunctions()}

        public ValueClassLiteralEvalImpl(Object[] references) {
          this.references = references;
          ${ctx.initMutableStates()}
        }

        public java.lang.Object apply(java.lang.Object z) {
          InternalRow ${ctx.INPUT_ROW} = (InternalRow) z;
          ${eval.code}
          return ${eval.isNull} ? ((Object)null) : ((Object)${eval.value});
        }
      }
    """

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments())
    )

    val (clazz, _) = CodeGenerator.compile(code)
    val codegen =
      clazz.generate(ctx.references.toArray).asInstanceOf[InternalRow => AnyRef]

    codegen(input)
  }

  def dataType: DataType = encoder.catalystRepr
  def children: Seq[Expression] = Nil

  override def genCode(ctx: CodegenContext): ExprCode =
    encoder.toCatalyst(Literal.create(obj.value, encoder.jvmRepr)).genCode(ctx)

  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = ???
}

object ValueClassLiteral {

  def lit[T, V <: AnyVal { def value: Any }](
      value: V
    )(implicit
      e: TypedEncoder[V]
    ): TypedColumn[T, V] =
    new TypedColumn[T, V](new ValueClassLiteral(value, e))
}
