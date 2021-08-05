package cchantep.sandbox

import org.apache.spark.sql.types._
import org.apache.spark.sql.{ functions => F }

import frameless._

final class EncoderSpec
    extends org.specs2.mutable.Specification
    with SharedSparkSession {

  "Encoder".title

  "Value class" should withSpark { implicit spark =>
    import spark.implicits._
    implicit val sparkDelay: SparkDelay[Job] = Job.framelessSparkDelayForJob

    "be read from scalar row" in {
      TypedDataset
        .createUnsafe[Name1](Seq("Lorem").toDF)
        .collect()
        .run() must_=== Seq(
        new Name1("Lorem")
      )
    } tag "ok"

    "be read as ClassClass1" in {
      val df = Seq("""{"name":"Foo"}""").toDF
        .withColumn(
          "foo",
          F.from_json(
            F.column("value"),
            StructType(
              Seq(
                StructField("name", StringType, true)
              )
            )
          )
        )
        .select("foo.*")

      TypedDataset.createUnsafe[CaseClass1](df).collect().run() must_=== Seq(
        CaseClass1(new Name1("Foo"))
        /* Test failure:

           Error while decoding: java.lang.RuntimeException: Couldn't find a valid constructor on class cchantep.sandbox.CaseClass1
         */
      )
    } tag "ko"

    "be read as Name2 from scalar row (without fieldEncoder)" in {
      TypedDataset
        .createUnsafe[Name2](Seq("Lorem").toDF)
        .collect()
        .run() must_=== Seq(
        new Name2("Lorem")
      )
    }

    "be read as ClassClass2" in {
      val df =
        Seq("""{"name":"Foo"}""", """{"name":"Bar","opt":"ipsum"}""").toDF
          .withColumn(
            "foo",
            F.from_json(
              F.column("value"),
              StructType(
                Seq(
                  StructField("name", StringType, false),
                  StructField("opt", StringType, true)
                )
              )
            )
          )
          .select("foo.*")

      implicit def fieldEncoder: TypedEncoder[Name2] = Name2.fieldEncoder

      import Name2.optEncoder

      implicit val encoder: TypedEncoder[CaseClass2] =
        TypedEncoder.usingDerivation

      TypedDataset.createUnsafe[CaseClass2](df).collect().run() must_=== Seq(
        CaseClass2(new Name2("Foo"), None),
        CaseClass2(new Name2("Bar"), Some(new Name2("ipsum")))
      )
    } tag "ok"

    "be read as ClassClass3" in {
      val df =
        Seq(
          """{"id":"XYZ"}""",
          """{"id":"ABC","lorem":{"name":"Bar"}}""",
          """{"id":"EFG","lorem":{"name":"ipsum","opt":"dolor"}}"""
        ).toDF
          .withColumn(
            "foo",
            F.from_json(
              F.column("value"),
              StructType(
                Seq(
                  StructField("id", StringType, false),
                  StructField(
                    "lorem",
                    StructType(
                      Seq(
                        StructField("name", StringType, false),
                        StructField("opt", StringType, true)
                      )
                    ),
                    true
                  )
                )
              )
            )
          )
          .select("foo.*")

      implicit def idEncoder: TypedEncoder[Id1] = Id1.fieldEncoder
      implicit def nameEncoder: TypedEncoder[Name2] = Name2.fieldEncoder

      import Name2.optEncoder

      implicit val encoder: TypedEncoder[CaseClass3] =
        TypedEncoder.usingDerivation

      TypedDataset.createUnsafe[CaseClass3](df).collect().run() must_=== Seq(
        CaseClass3(new Id1("XYZ"), None),
        CaseClass3(
          new Id1("ABC"),
          Some(CaseClass2(name = new Name2("Bar"), opt = None))
        ),
        CaseClass3(
          new Id1("EFG"),
          Some(
            CaseClass2(
              name = new Name2("ipsum"),
              opt = Some(new Name2("dolor"))
            )
          )
        )
      )
    } tag "ok"

    "set name column on CaseClass2 (failed)" in {
      val df =
        Seq("""{"name":"XYZ"}""").toDF
          .withColumn(
            "foo",
            F.from_json(
              F.column("value"),
              StructType(
                Seq(
                  StructField("name", StringType, false),
                  StructField("opt", StringType, true)
                )
              )
            )
          )
          .select("foo.*")

      implicit def nameEncoder: TypedEncoder[Name2] = Name2.fieldEncoder

      import Name2.optEncoder

      implicit val encoder: TypedEncoder[CaseClass2] =
        TypedEncoder.usingDerivation

      /* requirement failed: Literal must have a corresponding value to cchantep.sandbox.Name2, but class Some found. (literals.scala:215) */
      TypedDataset
        .createUnsafe[CaseClass2](df)
        .withColumnReplaced('name, functions.lit(new Name2("Foo")))
        .collect()
        .run()
        .headOption must beSome(CaseClass2(name = new Name2("Foo"), opt = None))

    } tag "ko"

    "set name column on CaseClass2 (successful)" in {
      val df =
        Seq("""{"name":"XYZ"}""").toDF
          .withColumn(
            "foo",
            F.from_json(
              F.column("value"),
              StructType(
                Seq(
                  StructField("name", StringType, false),
                  StructField("opt", StringType, true)
                )
              )
            )
          )
          .select("foo.*")

      implicit def nameEncoder: TypedEncoder[Name2] = Name2.fieldEncoder

      import Name2.optEncoder

      implicit val encoder: TypedEncoder[CaseClass2] =
        TypedEncoder.usingDerivation

      TypedDataset
        .createUnsafe[CaseClass2](df)
        .withColumnReplaced('name, ValueClassLiteral.lit(new Name2("Foo")))
        .collect()
        .run()
        .headOption must beSome(CaseClass2(name = new Name2("Foo"), opt = None))

    } tag "ok"
  }
}

// --- Datamodel

final class Name1(val value: String) extends AnyVal {
  override def toString = value
}

case class CaseClass1(name: Name1)

final class Name2(val value: String) extends AnyVal

object Name2 {
  import org.apache.spark.sql.types.{ DataType, StringType }
  import org.apache.spark.sql.catalyst.expressions._ //, objects._

  // Only when Value class is used as struct field
  def fieldEncoder: TypedEncoder[Name2] = new TypedEncoder[Name2] {
    val nullable: Boolean = true

    val jvmRepr: DataType = StringType

    val catalystRepr: DataType = StringType

    def fromCatalyst(path: Expression): Expression =
      TypedEncoder.stringEncoder.fromCatalyst(path)

    def toCatalyst(path: Expression): Expression =
      path // Invoke(path, "value", jvmRepr)
  }

  implicit def optEncoder: TypedEncoder[Option[Name2]] =
    new TypedEncoder[Option[Name2]] {
      import org.apache.spark.sql.catalyst.expressions._, objects._

      val nullable: Boolean = true

      val jvmRepr: DataType = ObjectType(classOf[Name2])

      val catalystRepr: DataType = StringType

      def toCatalyst(path: Expression): Expression =
        fieldEncoder.toCatalyst(UnwrapOption(catalystRepr, path))

      def fromCatalyst(path: Expression): Expression = {
        val javaValue = fieldEncoder.fromCatalyst(path)

        val value = NewInstance(
          classOf[Name2],
          Seq(javaValue),
          jvmRepr
        )

        WrapOption(value, jvmRepr)
      }
    }
}

case class CaseClass2(name: Name2, opt: Option[Name2])

final class Id1(val value: String) extends AnyVal

object Id1 {
  import org.apache.spark.sql.types.{ DataType, StringType }
  import org.apache.spark.sql.catalyst.expressions._, objects._

  // Only when Value class is used as struct field
  def fieldEncoder: TypedEncoder[Id1] = new TypedEncoder[Id1] {
    val nullable: Boolean = true

    val jvmRepr: DataType = StringType

    val catalystRepr: DataType = StringType

    def fromCatalyst(path: Expression): Expression =
      TypedEncoder.stringEncoder.fromCatalyst(path)

    def toCatalyst(path: Expression): Expression =
      Invoke(path, "value", jvmRepr)
  }
}

case class CaseClass3(id: Id1, lorem: Option[CaseClass2])
