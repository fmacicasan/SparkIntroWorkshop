package org.community.bigdata.workshop.sparkintro.movielens.processing;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.community.bigdata.workshop.sparkintro.movielens.functions.conversion.MovieConvertion;
import org.community.bigdata.workshop.sparkintro.movielens.functions.conversion.UserConversion;
import org.community.bigdata.workshop.sparkintro.movielens.model.Movie;
import org.community.bigdata.workshop.sparkintro.movielens.model.User;

/**
 * @author flo
 * @since 16/12/15.
 */
public class SortedMoviesByName {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName(SortedMoviesByName.class.getSimpleName());
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> records = sc.textFile("data/movielens/input/movies");

        JavaRDD<Movie> movies = records.map(new MovieConvertion());

        movies.sortBy(Movie::getMovieTitle, true, 1).foreach(SortedMoviesByName::dump);
    }


    private static void dump(Object object) {
        System.out.println(object);
    }
}
