package com.itheima.wordcount;

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
 * todo:利用java实现spark的wordcount程序
 */
public class WordCountJava {
    public static void main(String[] args) {

        SparkConf wordCountJava = new SparkConf().setAppName("WordCountJava").setMaster("local[2]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(wordCountJava);
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("E:/aa.txt");
        //切分每一行,获取所有单词,第一个参数是输入类型,第二个参数是输出类型
        JavaRDD<String> stringJavaRDD1 = stringJavaRDD.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                String[] split = s.split(" ");
                return Arrays.asList(split).iterator();
            }
        });
        //每个单词记为1
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = stringJavaRDD1.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        List<Tuple2<String, Integer>> collect = stringIntegerJavaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).collect();

        for (Tuple2<String, Integer> stringIntegerTuple2 : collect) {
            System.out.println(stringIntegerTuple2._1+"--"+stringIntegerTuple2._2 );
        }

        javaSparkContext.stop();

    }
}
