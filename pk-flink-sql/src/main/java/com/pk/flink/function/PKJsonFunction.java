package com.pk.flink.function;

import org.apache.flink.table.functions.ScalarFunction;
import org.json.JSONObject;

public class PKJsonFunction extends ScalarFunction{

    public String eval(String line, String key) {
        JSONObject jsonObject = new JSONObject(line);
        String result = "";

        if(jsonObject.has(key)) {
            return jsonObject.getString(key);
        }

        return result;
    }
}
