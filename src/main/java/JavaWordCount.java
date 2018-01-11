
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;
import scala.Tuple2;

public class JavaWordCount {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Spark COunt").setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        final int threshold = Integer.parseInt(args[1]);

        // split each document into words
        JavaRDD<String> tokenized = sc.textFile(args[0]).flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterable<String> call(String s) {
                        return Arrays.asList(s.split(" "));
                    }
                }
        );

        // count the occurrence of each word
        JavaPairRDD<String, Integer> counts = tokenized.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                }
        ).reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                }
        );

        System.out.println("RDD before filter");

        counts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2);
            }
        });

        // filter out words with less than threshold occurrences
        JavaPairRDD<String, Integer> filtered = counts.filter(
                new Function<Tuple2<String, Integer>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Integer> tup) {
                        System.out.println("Inside filter Value of tup._2 " + tup._2 + " Value of threshold " + threshold + " Value of expression " + (tup._2() >= threshold));
                        return tup._2() >= threshold;
                    }
                }

        );
        filtered.collect();

        System.out.println("RDD after filter");
        filtered.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2);
            }
        });

        // count characters
        JavaPairRDD<Character, Integer> charCounts = filtered.flatMap(
                new FlatMapFunction<Tuple2<String, Integer>, Character>() {
                    @Override
                    public Iterable<Character> call(Tuple2<String, Integer> s) {
                        Collection<Character> chars = new ArrayList<Character>(s._1().length());
                        for (char c : s._1().toCharArray()) {
                            chars.add(c);
                        }
                        return chars;
                    }
                }
        ).mapToPair(
                new PairFunction<Character, Character, Integer>() {
                    @Override
                    public Tuple2<Character, Integer> call(Character c) {
                        return new Tuple2<Character, Integer>(c, 1);
                    }
                }
        ).reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                }
        );

        System.out.println(charCounts.collect());
    }
}
