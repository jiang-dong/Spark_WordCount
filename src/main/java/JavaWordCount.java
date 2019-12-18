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

public class JavaWordCount {

    public static void main(String[] args) {
        //1、创建SparkContext
        SparkConf config = new SparkConf().setMaster("local[4]").setAppName("wordcount");
        JavaSparkContext sc = new JavaSparkContext(config);
        //2、读取数据
        JavaRDD<String> rdd1 = sc.textFile("data/wordcount.txt");
        //3、数据处理
        //3.1、切分 压平
        //第一个泛型代表的入参的类型
        //第二个泛型代表的返回值的类型
        JavaRDD<String> rdd2 = rdd1.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] arr = s.split(" ");
                List<String> list = Arrays.asList(arr);
                return list.iterator();
            }
        });
        //3.2、赋予词频
        //第一个泛型是入参的类型
        //第二个泛型是返回的元组的K的类型
        //第三个泛型是返回的元组的V的类型
        JavaPairRDD<String, Integer> rdd3 = rdd2.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        //3.3、聚合
        //第一个泛型代表函数的入参的第一个参数的类型
        //第二个泛型代表函数的入参的第二个参数的类型
        //第三个泛型代表函数返回值的类型
        JavaPairRDD<String, Integer> rdd4 = rdd3.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //3.4、按照单词的个数倒序排列
        JavaPairRDD<Integer, String> rdd5 = rdd4.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
                return new Tuple2<>(t._2, t._1);
            }
        });
        JavaPairRDD<Integer, String> rdd6 = rdd5.sortByKey(false);

        JavaPairRDD<String, Integer> rdd7 = rdd6.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
                return new Tuple2<>(t._2, t._1);
            }
        });
        //4、结果展示
        List<Tuple2<String, Integer>> result = rdd7.collect();
        for(Tuple2<String,Integer>  t:result){
            System.out.println(t._1+"--"+t._2);
        }
    }

}
