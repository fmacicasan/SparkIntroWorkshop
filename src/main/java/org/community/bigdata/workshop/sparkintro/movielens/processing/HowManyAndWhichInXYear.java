package org.community.bigdata.workshop.sparkintro.movielens.processing;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.community.bigdata.workshop.sparkintro.movielens.functions.conversion.MovieConvertion;
import org.community.bigdata.workshop.sparkintro.movielens.model.Movie;
import scala.Tuple2;

import java.sql.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author flo
 * @since 16/12/15.
 */
public class HowManyAndWhichInXYear {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName(HowManyAndWhichInXYear.class.getSimpleName());
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> records = sc.textFile("data/movielens/input/movies");

        JavaRDD<Movie> movies = records.map(new MovieConvertion());

        JavaPairRDD<Integer, Iterable<Movie>> moviesByYear = movies
                .groupBy(i ->
                        Optional.ofNullable(i.getReleaseDate())
                        .orElse(Date.valueOf("2020-12-12")).getYear() + 1900);
        moviesByYear.mapValues(HowManyAndWhichInXYear::toInfo)
                .sortByKey()
                .foreach(HowManyAndWhichInXYear::dump);

    }

    private static Tuple2<Integer, List<Movie>> toInfo(Iterable<Movie> movies) {
        List<Movie> stream = StreamSupport.stream(movies.spliterator(), false).collect(Collectors.toList());
        return new Tuple2<>(stream.size(), stream);
    }

    private static void dump(Object object) {
        System.out.println(object);
    }
}
