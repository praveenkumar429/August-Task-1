package com.example.kafakaspark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import static org.apache.hadoop.hdfs.server.namenode.ListPathsServlet.df;

/**
 * Created by anya on 7/2/17.
 */
public class JavaDataFrames {
    public static void main(String[] args) {
          JavaSparkContext jsc = new JavaSparkContext(
                  new SparkConf()
                          .setMaster("local")
                          .setAppName("DataFrames"));
        SQLContext javaSQLCtx=new SQLContext(jsc);

        Dataset<Row> ds= javaSQLCtx.read().json("people.json");
        ds.show();
        // Print the schema in a tree format
        ds.printSchema();
        // Select only the "name" column
        ds.select("name").show();
        // Select everybody, but increment the age by 1
        ds.select(ds.col("name"), ds.col("age").plus(1)).show();
        // Select people older than 21
        ds.filter(ds.col("age").gt(21)).show();
        // Count people by age
        ds.groupBy("age").count().show();


        ds.createOrReplaceTempView("people");
        Dataset<Row> jDF=javaSQLCtx.sql("select * from people");
        jDF.show();



    }
}
