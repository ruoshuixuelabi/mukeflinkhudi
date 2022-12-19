package com.pk.flink.function;

import com.pk.flink.bean.Access;
import org.apache.flink.api.java.functions.KeySelector;

public class AccessDomainKeySelector implements KeySelector<Access, String> {
    @Override
    public String getKey(Access value) throws Exception {
        return value.getDomain();
    }
}
