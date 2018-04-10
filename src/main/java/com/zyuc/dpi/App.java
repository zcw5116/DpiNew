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

        Dataset<Row> dataframe = sparkSession.sql("select * from tmp");
        List<Tuple2<Integer,Integer>> list = dataframe.toJavaRDD()
                .mapToPair((Row row) -> new Tuple2<Integer, String>(row.getInt(0), row.getString(1)))
                .groupByKey(5)
                .mapToPair((Tuple2<Integer, Iterable<String>> tuple) -> {
                    int id = tuple._1();
                    AtomicInteger atomicInteger = new AtomicInteger(0);
                    tuple._2().forEach((String name) -> atomicInteger.incrementAndGet());
                    return new Tuple2<Integer, Integer>(id, atomicInteger.get());
                }).collect();
       ListIterator iter =  list.listIterator();
        while(iter.hasNext()){
            System.out.println(iter.next());
        }

        sparkSession.stop();
        sparkSession.close();


    }
}
