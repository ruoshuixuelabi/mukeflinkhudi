package com.pk.flink.transformation;

import com.pk.flink.bean.Access;
import com.pk.flink.function.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class TransformationApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /*
         * map：每个元素都作用上一个方法，得到新的元素
         * 需求：读取access.log的数据，返回的是一堆Access对象：一行数据==>一个access对象
         */
//        DataStreamSource<String> source = env.readTextFile("data/access.log");
        //上面一行的代码已经废弃了，新版本使用下面的方式来实现同样的功能
//        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("data/access.log")).build();
//        DataStreamSource<String> source =
//                env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-source");
//        SingleOutputStreamOperator<Access> mapStream = source.map(new AccessConvertFunction());
//        mapStream.print();
        /*
         * filter：过滤出满足条件的元素
         */
//        DataStreamSource<String> source = env.readTextFile("data/access.log");
//        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("data/access.log")).build();
//        DataStreamSource<String> source =
//                env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-source");
//        source.map(new AccessConvertFunction()).filter(x -> x.getTraffic() > 4000).print();
        /*
         * flatMap：可以是一对一、一对多、一对0  一个元素进来，可以出去0、1、多个元素
         */
//        DataStreamSource<String> source = env.readTextFile("data/access.log");
//        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("data/access.log")).build();
//        DataStreamSource<String> source =
//                env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-source");
//        source.map(new AccessConvertFunction())
//                .flatMap(new AccessFlatMapFunction())
//                .print();
        /*
         * keyBy  聚合
         * sum reduce...
         */
//        DataStreamSource<String> source = env.readTextFile("data/access.log");
//        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("data/access.log")).build();
//        DataStreamSource<String> source = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-source");
//        source.map(new AccessConvertFunction())
//                .keyBy(Access::getDomain).sum("traffic")//按照getDomain进行分组，然后求traffic的和
//                .print();
        /*
         * union：合并多个流
         * 数据类型问题：union的多个流中数据类型是需要相同的
         * 数据类型相同的多流操作
         */
//        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3);
//        DataStreamSource<Integer> stream2 = env.fromElements(11, 12, 13);
//        DataStreamSource<String> stream3 = env.fromElements("A", "B", "C");
//        stream1.union(stream2).map(x -> "PK_" + x).print();
//        stream1.union(stream1).print();
//        stream1.union(stream1, stream2).print();
        /*
         * connect: 数据类型可以不同
         * 两流的操作
         * 只是形式的连接
         */
//        ConnectedStreams<Integer, String> connectedStreams = stream1.connect(stream3);
//        connectedStreams.map(new CoMapFunction<Integer, String, String>() {
//            // 共享状态
//            String prefix = "PK_";
//
//            /**
//             * 对第一个流的操作
//             */
//            @Override
//            public String map1(Integer value) throws Exception {
//                return prefix + value * 10;
//            }
//
//            // 对第二个流的操作
//            @Override
//            public String map2(String value) throws Exception {
//                return prefix + value.toLowerCase();
//            }
//        }).print();
        /*
         * 分区器
         */
//        env.setParallelism(3);
//        DataStreamSource<String> source = env.readTextFile("data/access.log");
//        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("data/access.log")).build();
//        DataStreamSource<String> source = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-source");
//        source.map(new AccessConvertFunction())
//                .map(new MapFunction<Access, Tuple2<String, Access>>() {
//                    @Override
//                    public Tuple2<String, Access> map(Access value) throws Exception {
//                        // 按照域名作为分区的key
//                        return Tuple2.of(value.getDomain(), value);  // Tuple2<String, Access>
//                    }
//                }) // 做分区之前 是需要整理成Tuple类型
//                .partitionCustom(new PKPartitioner(), x -> x.f0)
//                // 下面的这段map方法目的是验证：相同的域名是否真的在同一个分区内，看threadid是否相同即可
//                .map(new MapFunction<Tuple2<String, Access>, Access>() {
//                    @Override
//                    public Access map(Tuple2<String, Access> value) throws Exception {
//                        System.out.println("current thread id is " + Thread.currentThread().getId() + ", value is:" + value);
//                        return value.f1;
//                    }
//                }).print();
        /*
         * 分流操作：把一个分拆分成多个流
         * split 在老的flink版本中是有的，但是新的版本中已经没有这个api
         * 那就说明新版本肯定提供了更好用的使用方式：side output
         */
//        DataStreamSource<String> source = env.readTextFile("data/access.log");
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("data/access.log")).build();
        DataStreamSource<String> source = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-source");
        SingleOutputStreamOperator<Access> stream = source.map(new AccessConvertFunction());
//        SingleOutputStreamOperator<Access> pk1Stream = stream.filter(x -> "pk1.com".equals(x.getDomain()));
//        SingleOutputStreamOperator<Access> pk2Stream = stream.filter(x -> "pk2.com".equals(x.getDomain()));
//        pk1Stream.print("域名是pk1.com的流");
//        pk2Stream.print("域名是pk2.com的流");
        OutputTag<Access> pk1OutputTag = new OutputTag<Access>("pk1") {};
        OutputTag<Access> pk2OutputTag = new OutputTag<Access>("pk2") {};
        SingleOutputStreamOperator<Access> processStream = stream.process(new ProcessFunction<Access, Access>() {
            @Override
            public void processElement(Access value, Context ctx, Collector<Access> out) throws Exception {
                if ("pk1.com".equals(value.getDomain())) {
                    ctx.output(pk1OutputTag, value); // pk1.com的走pk1的OutputTag
                } else if ("pk2.com".equals(value.getDomain())) {
                    ctx.output(pk2OutputTag, value);  // pk2.com的走pk2的OutputTag
                } else {
                    out.collect(value); // pk3.com的走主流
                }
            }
        });
        processStream.print("主流:");
        processStream.getSideOutput(pk1OutputTag).print("PK1的：");
        processStream.getSideOutput(pk2OutputTag).print("PK2的：");
        env.execute("作业名字");
    }
}