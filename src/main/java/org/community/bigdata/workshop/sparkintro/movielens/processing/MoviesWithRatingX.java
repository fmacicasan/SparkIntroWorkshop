package org.community.bigdata.workshop.sparkintro.movielens.processing;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.community.bigdata.workshop.sparkintro.movielens.functions.conversion.MovieConvertion;
import org.community.bigdata.workshop.sparkintro.movielens.functions.conversion.RatingConvertion;
import org.community.bigdata.workshop.sparkintro.movielens.functions.conversion.UserConversion;
import org.community.bigdata.workshop.sparkintro.movielens.model.Movie;
import org.community.bigdata.workshop.sparkintro.movielens.model.Rating;
import org.community.bigdata.workshop.sparkintro.movielens.model.User;
import scala.Tuple2;

import java.util.List;

/**
 * Created by tudor on 12/6/2015.
 */
public class MoviesWithRatingX {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("SparkPairTransformation");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> movie_records = sc.textFile("data/movielens/input/movies");
        JavaRDD<String> rating_records = sc.textFile("data/movielens/input/ratings");

        JavaRDD<Movie> movies = movie_records.map(new MovieConvertion());
        JavaRDD<Rating> ratings = rating_records.map(new RatingConvertion());

        JavaPairRDD<Integer, Rating> ratingPairRDD = ratings.mapToPair(rating ->
                new Tuple2<Integer, Rating>(rating.getUserId(), rating));
        JavaPairRDD<Integer, Movie> moviePairRDD = movies.mapToPair(rating ->
                new Tuple2<Integer, Movie>(rating.getId(), rating));

        JavaPairRDD<Integer, Tuple2<Movie, Rating>> join = moviePairRDD.join(ratingPairRDD);
        List<Tuple2<Integer, Tuple2<Movie, Rating>>> rating8 = join.filter(i -> Double.compare(i._2()._2().getRating(), 3d) == 0).collect();

        for (Tuple2<Integer, Tuple2<Movie, Rating>> r8 : rating8) {
            System.out.println(r8);
        }

        try {
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
