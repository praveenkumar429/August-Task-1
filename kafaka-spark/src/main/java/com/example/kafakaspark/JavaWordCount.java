package com.example.kafakaspark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by anya on 6/2/17.
 */
public class JavaWordCount {
    public static void main(String[] args) {
        //Create spark context
        //JavaSparkContext jsc= new JavaSparkContext(new SparkConf().setMaster("spark://anya-Inspiron-5559:7077").setAppName("JavaWordCount"));
        JavaSparkContext jsc= new JavaSparkContext(new SparkConf().setMaster("local").setAppName("JavaWordCount"));
        //Create RDD and by loading some text file.
        JavaRDD<String> jrdd= jsc.textFile("wordcount.txt");

        //split each document into word
        JavaRDD<String> word=jrdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        //Count the occurence of word in map
        JavaPairRDD<String, Integer> countWord= word.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        });

        //Combined all word using reduce function.
        JavaPairRDD<String, Integer> reduceWord=countWord.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });

        List<Tuple2<String, Integer>> output = reduceWord.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        jsc.stop();




    }

}
