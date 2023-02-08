package com.pk.flink.source;

import com.pk.flink.bean.Access;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * 自定义数据源
 * 多并行度的
 */
public class AccessSourceV2 implements ParallelSourceFunction<Access> {
    boolean isRunning = true;

    /**
     * 造数据是自定义数据源的使用方式之一
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Access> ctx) throws Exception {
        Random random = new Random();
        String[] domains = {"pk1.com", "pk2.com", "pk3.com"};
        while (isRunning) {
            long time = System.currentTimeMillis();
            ctx.collect(new Access(time, domains[random.nextInt(domains.length)], random.nextInt(1000) + 1000));
            Thread.sleep(5000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}