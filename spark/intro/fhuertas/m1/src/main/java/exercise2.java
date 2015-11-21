/**
 * Created by Francisco Huertas on 11/11/15.
 */

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class exercise2 {
    private static final Logger LOGGER = LogManager.getLogger(exercise2.class.getName());

    public static void main(String[] args) {
        Map<String, String> properties = Common.readProperties(args);
        JavaSparkContext context = ContextSingleton.getContextFromProps(properties);

        String filePath = properties.get(Common.FILE_PATH_ATTR);
        if (filePath == null){
            filePath = "src/main/resources/movies.dat";
            LOGGER.log(Level.INFO,"file Path is not defined with --"+Common.FILE_PATH_ATTR+", using "+filePath);
        }

        // solution 1 (two steps)
        ArrayList a = null;



        JavaRDD<String> films= context.textFile(filePath)
                // films with year
                .map(line -> line.split("::")[1])
                // remove year
                .map(line ->
                        line.substring(0,line.lastIndexOf("(")));
        Tuple2<String,Integer> popularWord = films
                // words
                .flatMap(line->Arrays.asList(line.replace(" +", " ").trim().split(" ")))
                .filter(word->word.length() > 3)
                .mapToPair(word->new Tuple2<>(word,1))
                // count
                .reduceByKey((n1,n2)->n1+n2)
                // the most popular
                .reduce((w1,w2)-> (w1._2() > w2._2())?w1:w2);
        System.out.println("The most popular word="+popularWord._1()+", appearances="+popularWord._2());
        Broadcast<String> bPopularWord = context.broadcast(popularWord._1());
        List<String> filmsWithWord = films.filter(film->film.contains(bPopularWord.getValue())).collect();

        System.out.println(Joiner.on("\n").join(filmsWithWord));


        // solution 2

        Object result =
                films.mapToPair(film->new Tuple2<>(film,Sets.newHashSet(film.replace(" +"," ").split(" "))))
                        .flatMapValues(set->set)
                        .filter(t2->t2._2().length()>3)
                        .mapToPair(elem->new Tuple2<>(elem._2(),Sets.newHashSet(elem._1())))
                        .groupByKey()
                        .reduce((pair1,pair2)-> (Iterables.size(pair1._2())>Iterables.size(pair2._2()))?pair1:pair2);

        System.out.println(result);

        // INCORRECTO
        result = films.mapToPair(film->new Tuple2<>(film,Sets.newHashSet(film.replace(" +"," ").split(" "))))
                .flatMapValues(set->set)
                .mapToPair(pair->pair.swap())
                .aggregateByKey(
                        new Tuple2<>(0,Sets.<String>newIdentityHashSet()),
                        (tuple,string)-> new Tuple2(1,Sets.newHashSet(string))
                        ,(t1,t2)->
                                new Tuple2<>(t1._1()+t2._1(),Stream.concat(t1._2().stream(),t2._2().stream()).collect(Collectors.toSet())))
                .reduce((t1,t2)->(t1._2()._1() > t2._2()._1())?t1:t2);




        System.out.println(result);

    }
}
