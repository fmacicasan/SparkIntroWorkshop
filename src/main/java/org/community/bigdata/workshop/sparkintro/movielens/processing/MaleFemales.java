package org.community.bigdata.workshop.sparkintro.movielens.processing;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.community.bigdata.workshop.sparkintro.movielens.functions.conversion.UserConversion;
import org.community.bigdata.workshop.sparkintro.movielens.functions.wordcount.WordPairFunction;
import org.community.bigdata.workshop.sparkintro.movielens.functions.wordcount.WordSeparatorFunction;
import org.community.bigdata.workshop.sparkintro.movielens.functions.wordcount.WordcountFunction;
import scala.Tuple2;

/**
 * @author flo
 * @since 16/12/15.
 */
public class MaleFemales {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName(MaleFemales.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> records = sc.textFile("data/movielens/input/users");
        JavaRDD<String> genders = records.map(new UserConversion()).map(i -> i.getGender());

        System.out.println(genders.countByValue());


        try {
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
