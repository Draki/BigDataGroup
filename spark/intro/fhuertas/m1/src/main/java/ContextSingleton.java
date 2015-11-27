/**
 * Created by Francisco Huertas on 10/11/15.
 */

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Map;
import java.util.NoSuchElementException;

public class ContextSingleton {


    private static final Logger LOGGER = LogManager.getLogger(ContextSingleton.class.getName());

    private static final String SPARK_MASTER_ATTR = "spark.master";
    private static final String SPARK_APP_NAME_ATTR = "spark.app.name";

    private static JavaSparkContext context = null;

    public static JavaSparkContext getContextFromProps(Map<String,String> properties) {
        if (context != null) {

            return context;
        }



        if (properties.get(Common.MASTER) == null) {
            context = ContextSingleton.getContext("ejercicio1");
        } else {
            context = ContextSingleton.getContext("ejercicio1",properties.get(Common.MASTER));
        }
        return context;
    }


    public static JavaSparkContext getContext(String appName) {
        if (context != null) {
            return context;
        }
        SparkConf conf = new SparkConf().setAppName(appName).setMaster("local[2]");
        try {
            String master = conf.get(SPARK_MASTER_ATTR);
            if (master.equals("local")){
                LOGGER.log(Level.ERROR,"This application needs at least two cores \"local[2]\"");
                throw new RuntimeException("This application needs at least two cores \"local[2]\"");
            }
        } catch (NoSuchElementException ex) {
            LOGGER.log(Level.INFO, "Master is not defined. It is set to local[2].");
            conf.setMaster("local[2]");
        }
        return getContext(conf);
    }
    public static JavaSparkContext getContext(String appName,String master) {
        if (context != null) {
            return context;
        }
        return getContext(new SparkConf().setAppName(appName).setMaster(master));

    }

    private static JavaSparkContext getContext(SparkConf conf){
        context = new JavaSparkContext(conf);
        return context;
    }
}
