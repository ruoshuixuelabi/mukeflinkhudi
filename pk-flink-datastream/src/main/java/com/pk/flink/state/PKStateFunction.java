package com.pk.flink.state;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

public class PKStateFunction implements MapFunction<String, String>, CheckpointedFunction {
    ListState<String> listState;
    FastDateFormat format = FastDateFormat.getInstance("hh:MM:ss");

    @Override
    public String map(String value) throws Exception {
        System.out.println("map");
        if (value.contains("pk")) {
            throw new RuntimeException("大家快跑，PK哥来了，这是个狠人...");
        } else {
            listState.add(value);
        }
        StringBuilder builder = new StringBuilder();
        for (String s : listState.get()) {
            builder.append(s);
        }
        return builder.toString();
    }

    /**
     * 会被周期性执行的
     * 目前来看是 env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE); 的时间
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        System.out.println(format.format(System.currentTimeMillis()) + " ==> snapshotState   " + context.getCheckpointId());
    }

    /**
     * 初始化操作
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        System.out.println("initializeState");
        OperatorStateStore operatorStateStore = context.getOperatorStateStore();
        listState = operatorStateStore.getListState(
                new ListStateDescriptor<String>("list", String.class)
        );
    }
}