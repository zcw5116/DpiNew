package com.zyuc.dpi;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        SparkSession sparkSession = SparkSession.builder()
                .appName("test").master("local[*]").enableHiveSupport().getOrCreate();

        sparkSession.stop();
        sparkSession.close();


    }
}
