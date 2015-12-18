package org.community.bigdata.workshop.sparkintro.movielens.processing;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.community.bigdata.workshop.sparkintro.movielens.functions.conversion.MovieConvertion;
import org.community.bigdata.workshop.sparkintro.movielens.functions.conversion.RatingConvertion;
import org.community.bigdata.workshop.sparkintro.movielens.functions.conversion.UserConversion;
import org.community.bigdata.workshop.sparkintro.movielens.model.Movie;
import org.community.bigdata.workshop.sparkintro.movielens.model.Rating;
import org.community.bigdata.workshop.sparkintro.movielens.model.User;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

/**
 * Created by tudor on 12/6/2015.
 */
public class MoviesWithRatingX implements Serializable {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("MoviesWithRatingX");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        double targetRating = 3d;

        JavaRDD<String> movie_records = sc.textFile("data/movielens/input/movies");
        JavaRDD<String> rating_records = sc.textFile("data/movielens/input/ratings");

        JavaRDD<Movie> movies = movie_records.map(new MovieConvertion());
        JavaRDD<Rating> ratings = rating_records.map(new RatingConvertion());

        JavaPairRDD<Integer, Rating> ratingPairRDD =
                ratings.mapToPair(rating -> new Tuple2<>(rating.getMovieId(), rating));
        JavaPairRDD<Integer, Movie> moviePairRDD =
                movies.mapToPair(rating -> new Tuple2<>(rating.getId(), rating));

        JavaPairRDD<Integer, Tuple2<Movie, Rating>> join = moviePairRDD.join(ratingPairRDD);
        JavaPairRDD<Integer, Tuple2<Movie, Rating>> filtered =
                join.filter(i -> Double.compare(i._2()._2().getRating(), targetRating) == 0);

        filtered.map(i -> i._2()._1().getMovieTitle())
                .distinct()
                .foreach(MoviesWithRatingX::dump);
    }

    //TODO: investigate why explicit method reference to System.out::println returns serialization exception
    private static void dump(String someString) {
        System.out.println(someString);
    }
}
