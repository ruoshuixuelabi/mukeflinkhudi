package com.pk.flink.function;

import com.pk.flink.bean.Access;
import org.apache.flink.api.common.functions.RichFilterFunction;

public class AccessFilterFunction extends RichFilterFunction<Access>{
    @Override
    public boolean filter(Access value) throws Exception {
        return value.getTraffic() > 4000;
    }
}
