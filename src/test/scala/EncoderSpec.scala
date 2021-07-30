package cchantep.sandbox

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

    "be read from unsafe dataframe as CaseClass1" in {
      val df = Seq("Foo").toDF.select(F.struct(F.column("value").as("name")))

      TypedDataset.createUnsafe[CaseClass1](df).collect().run() must_=== Seq(
        CaseClass1(new Name1("Foo"))
        /* Test failure:

           Error while decoding: java.lang.RuntimeException: Couldn't find a valid constructor on class cchantep.sandbox.CaseClass1
         */
      )
    } tag "wip"

    "be read from safe dataset as CaseClass1" in {
      TypedDataset
        .create[CaseClass1](Seq(CaseClass1(new Name1("Foo"))))
        .collect()
        .run() must_=== Seq(
        CaseClass1(new Name1("Foo"))
        /* Test failure:

           java.lang.ClassCastException: java.base/java.lang.String cannot be cast to cchantep.sandbox.Name1
         */
      )
    }
  }
}

// --- Datamodel

final class Name1(val value: String) extends AnyVal {
  override def toString = value
}

case class CaseClass1(name: Name1)
