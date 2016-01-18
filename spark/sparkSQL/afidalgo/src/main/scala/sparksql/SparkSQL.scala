package sparksql

import org.apache.spark.SparkContext
import org.apache.spark.sql._


class SparkSQL (sparkContext: SparkContext){


    val sqlContext = new SQLContext(sparkContext)

    def executeQuery(query:String):DataFrame={

        sqlContext.sql(query)
    }

}
