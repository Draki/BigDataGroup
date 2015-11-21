/**
 * Created by Francisco Huertas on 10/11/15.
 */

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Map;

public class exercise1 {
    private static final Logger LOGGER = LogManager.getLogger(exercise1.class.getName());






    public static void main(String []args) {
        Map<String,String> properties = Common.readProperties(args);
        JavaSparkContext context = ContextSingleton.getContextFromProps(properties);

        String filePath =  properties.get(Common.FILE_PATH_ATTR);
        if (filePath == null){
            filePath = "src/main/resources/movies.dat";
            LOGGER.log(Level.INFO,"file Path is not defined with --"+Common.FILE_PATH_ATTR+", using "+filePath);
        }

        JavaRDD<String> rddFile = context.textFile(filePath);


//        JavaPairRDD<String,Integer> filmPerYear =
        Tuple2<String, Integer> popularYear =
                rddFile.map(line->line.split("::")[1])
                        .mapToPair(line -> {
                            String []film = line.split("\\(");
                            String year = film[film.length-1].substring(0,film[film.length-1].indexOf(")"));
                            return new Tuple2<>(year,1);
                        })
                        .reduceByKey((c1,c2)->c1+c2)
                        .reduce((f1,f2)-> (f1._2() > f2._2()?f1:f2))
//                        .max((f1,f2)->f1._2().compareTo(f2._2())) Problemas de serialización
                ;
        System.out.println(popularYear._1() +" - " + popularYear._2());
    }
}
