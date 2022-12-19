package com.pk.flink.function;

import com.pk.flink.bean.Access;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class PKConsoleSinkFunction extends RichSinkFunction<Access> {

    int subTaskId;

    // num>
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        subTaskId = getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public void invoke(Access value, Context context) throws Exception {
        System.out.println(subTaskId + 1 + "> " + value);
    }
}
