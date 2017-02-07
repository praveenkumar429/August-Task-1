package com.example.kafakaspark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by anya on 7/2/17.
 */
public class Java8WordCount {
    public static void main(String[] args) {
        //JavaSparkContext jsc= new JavaSparkContext(new SparkConf().setMaster("spark://anya-Inspiron-5559:7077").setAppName("JavaWordCount"));
        JavaSparkContext jsc = new JavaSparkContext(new SparkConf().setMaster("local").setAppName("JavaWordCount"));

        //Create RDD and by loading some text file.
        JavaRDD<String> jrdd = jsc.textFile("user.txt")
                .flatMap(line->Arrays.asList(line.split(" ")).iterator());
        jrdd.foreach(line-> System.out.println(line));

        JavaPairRDD<String, Integer> counts = jrdd.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
                .reduceByKey((x, y) -> x + y);

//        JavaRDD<String> jrdd = jsc.textFile("wordcount.txt");
//        JavaRDD<String> word = jrdd.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
//        JavaPairRDD<String, Integer> counts = word.mapToPair(w -> new Tuple2<String, Integer>(w, 1)).reduceByKey((x, y) -> x + y);

        counts.foreach(tuple -> System.out.println(tuple._1() + "\t " + tuple._2()));
        jsc.stop();
    }
}