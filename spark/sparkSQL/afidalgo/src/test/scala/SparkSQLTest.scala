import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest._
import sparksql.SparkSQL
import sparksql.Context


class SparkSQLTest extends FlatSpec with Matchers {

  "A spark SQL " should " can be execute queries in cassandra" in {

    val scContext = new Context("local[*]","spark://127.0.0.1:7077")

    val sparkSQL = new SparkSQL(scContext.context)
    val dataFrame = sparkSQL.executeQuery("select * from demo.users")
    val rows = dataFrame.collect()
    rows.length should be (2)
  }
}