package com.pk.flink.source;

import com.pk.flink.bean.Access;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.NumberSequenceIterator;

/**
 * Flink中datasource的使用
 */
public class FlinkDataSourceApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStreamSource<Long> source = env.fromParallelCollection(new NumberSequenceIterator(1, 10), Long.TYPE);
//        DataStreamSource<Long> source = env.fromSequence(1, 10);
//        System.out.println(source.getParallelism());
//        SingleOutputStreamOperator<Long> mapStream = source.map(x -> x + 100).setParallelism(3);
//        System.out.println(mapStream.getParallelism());
//        DataStreamSource<String> source = env.readFile(new TextInputFormat(null), "data/wc.data");
//        // 这个readTextFile方法底层其实调用的就是readFile
////        DataStreamSource<String> source = env.readTextFile("data/wc.txt");
//        System.out.println(source.getParallelism());  // 8
//        SingleOutputStreamOperator<String> mapStream = source.map(String::toUpperCase);
//        System.out.println(mapStream.getParallelism());
//        mapStream.print();
//        DataStreamSource<Access> source = env.addSource(new AccessSourceV2()).setParallelism(3); // 对于ParallelSourceFunction是可以根据具体情况来设定并行度的
//        System.out.println(source.getParallelism());
//        source.print();
        /*
         * 使用Flink自定义MySQL的数据源，进而读取MySQL里面的数据
         */
        env.addSource(new PKMySQLSource()).setParallelism(3).print();
        /*
         * 单并行度：fromElements  fromCollection  socketTextStream
         * 多并行度：readTextFile fromParallelCollection generateSequence  readFile
         * 自定义：
         */
        env.execute("作业名字");
    }
}