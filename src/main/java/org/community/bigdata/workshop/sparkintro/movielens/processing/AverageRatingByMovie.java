package org.community.bigdata.workshop.sparkintro.movielens.processing;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.community.bigdata.workshop.sparkintro.movielens.functions.conversion.MovieConvertion;
import org.community.bigdata.workshop.sparkintro.movielens.functions.conversion.RatingConvertion;
import org.community.bigdata.workshop.sparkintro.movielens.model.Movie;
import org.community.bigdata.workshop.sparkintro.movielens.model.Rating;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Optional;
import java.util.stream.StreamSupport;

/**
 * @author flo
 * @since 18/12/15.
 */
public class AverageRatingByMovie {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("AverageRatingByMovie");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> movie_records = sc.textFile("data/movielens/input/movies");
        JavaRDD<String> rating_records = sc.textFile("data/movielens/input/ratings");

        JavaRDD<Movie> movies = movie_records.map(new MovieConvertion());
        JavaRDD<Rating> ratings = rating_records.map(new RatingConvertion());

        JavaPairRDD<Integer, Rating> ratingPairRDD =
                ratings.mapToPair(rating -> new Tuple2<>(rating.getMovieId(), rating));
        JavaPairRDD<Integer, Movie> moviePairRDD =
                movies.mapToPair(movie -> new Tuple2<>(movie.getId(), movie));

        JavaPairRDD<Integer, Tuple2<Movie, Rating>> join = moviePairRDD.join(ratingPairRDD);

        //java 8 way with hacky compute average
        join.groupByKey()
                .map(AverageRatingByMovie::computeAverage)
                .sortBy(Tuple2::_2, true, 4)
                .foreach(AverageRatingByMovie::dump);

        //TODO: spark way w/ aggregate ?
    }

    //TODO: investigate why explicit method reference to System.out::println returns serialization exception
    private static void dump(Object someString) {
        System.out.println(someString);
    }

    private static Tuple2<String, Double> computeAverage(Tuple2<Integer, Iterable<Tuple2<Movie, Rating>>> tuple) {
        Iterable<Tuple2<Movie, Rating>> ratings = tuple._2();
        double average = StreamSupport.stream(ratings.spliterator(), false)
                .mapToDouble(i -> i._2().getRating())
                .average().orElse(0d);
        String name = ratings.iterator().hasNext() ?  ratings.iterator().next()._1().getMovieTitle():"default";
        return new Tuple2<>(name, average);
    }
}
