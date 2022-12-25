package com.pk.flink.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
//import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CheckpointApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 开启了chk之后，策略是Integer.MAX_VALUE  基本无限重启
        env.enableCheckpointing(5000);
//        RestartStrategies.RestartStrategyConfiguration restartStrategy = RestartStrategies.noRestart();
//        env.setRestartStrategy(restartStrategy);
        env.setStateBackend(new HashMapStateBackend());
//        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        // 设置 重启最大次数  两次重启之间的延迟间隔
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5));
        // 故障越频繁，两次重启之间的间隔就会越长
        /*
         * @param initialBackoff Starting duration between restarts 重启间隔时长的初始值  1s
         * @param maxBackoff The highest possible duration between restarts   重启间隔最大值：60s
         * @param backoffMultiplier Delay multiplier how many times is the delay longer than before  2
         * @param resetBackoffThreshold How long the job must run smoothly to reset the time interval
         * @param jitterFactor How much the delay may differ (in percentage)
         */
        env.setRestartStrategy(RestartStrategies.exponentialDelayRestart(
                Time.milliseconds(1),
                Time.milliseconds(1000),
                1.1, // exponential multiplier
                Time.milliseconds(2000), // threshold duration to reset delay to its initial value
                0.1 // jitter
        ));
        env.socketTextStream("localhost", 9527)
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        if (value.contains("pk")) {
                            throw new RuntimeException("PK哥来了，快跑...");
                        } else {
                            return value.toLowerCase();
                        }
                    }
                }).print();
        env.execute();
    }
}