package com.pk.flink.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink中datasource的使用
 */
public class FlinkDataSourceApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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