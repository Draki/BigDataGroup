package sparksql

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra.CassandraSQLContext


class SparkSQL (sparkContext: SparkContext){



    val cassandraContext = new CassandraSQLContext(sparkContext);


    def executeQuery(query:String):DataFrame={

        cassandraContext.sql(query)
    }

}
