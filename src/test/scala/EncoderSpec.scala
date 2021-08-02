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
    }

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
    }

    "be read as Name2 from scalar row" in {
      TypedDataset
        .createUnsafe[Name2](Seq("Lorem").toDF)
        .collect()
        .run() must_=== Seq(
        new Name2("Lorem")
      )
    }

    "be read as ClassClass2" in {
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

      implicit val nameFieldEncoder = Name2.encoder

      TypedDataset.createUnsafe[CaseClass2](df).collect().run() must_=== Seq(
        CaseClass2(new Name2("Foo"))
      )
    }
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
  import org.apache.spark.sql.catalyst.expressions._, objects._

  def encoder: TypedEncoder[Name2] = new TypedEncoder[Name2] {
    val nullable: Boolean = true

    val jvmRepr: DataType = StringType

    val catalystRepr: DataType = StringType

    def fromCatalyst(path: Expression): Expression = {
      println(s"path: ${path.getClass} = $path")
      val str = TypedEncoder.stringEncoder.fromCatalyst(path)
      //NewInstance(classOf[Name2], Seq(str), jvmRepr, nullable)
      str
    }

    def toCatalyst(path: Expression): Expression = {
      println(s"toCatalyst: $path")
      Invoke(path, "value", jvmRepr)
    }
  }
}

case class CaseClass2(name: Name2)
