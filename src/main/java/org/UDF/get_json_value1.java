package org.UDF;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.util.ArrayList;
import java.util.List;

public class get_json_value1 extends UDF{


    private static void findValues(JSONObject jsonObject, String targetKey, List<String> values) {
        if (jsonObject.has(targetKey)) {
            values.add(jsonObject.get(targetKey).toString());
        }

        for (String key : jsonObject.keySet()) {
            Object value = jsonObject.get(key);
            if (value instanceof JSONObject) {
                findValues((JSONObject) value, targetKey, values);
            } else if (value instanceof JSONArray) {
                findValues((JSONArray) value, targetKey, values);
            }
        }
    }

    private static void findValues(JSONObject jsonObject, String targetKey, String compareKey, String compareValue, List<String> values) {
        if (jsonObject.has(targetKey) && (compareKey == null || (jsonObject.has(compareKey) && jsonObject.get(compareKey).toString().equals(compareValue)))) {
            values.add(jsonObject.get(targetKey).toString());
        }

        for (String key : jsonObject.keySet()) {
            Object value = jsonObject.get(key);
            if (value instanceof JSONObject) {
                findValues((JSONObject) value, targetKey, compareKey, compareValue, values);
            } else if (value instanceof JSONArray) {
                findValues((JSONArray) value, targetKey, compareKey, compareValue, values);
            }
        }
    }

    private static void findValues(JSONArray jsonArray, String targetKey, List<String> values) {
        for (int i = 0; i < jsonArray.length(); i++) {
            Object item = jsonArray.get(i);
            if (item instanceof JSONObject) {
                findValues((JSONObject) item, targetKey, values);
            } else if (item instanceof JSONArray) {
                findValues((JSONArray) item, targetKey, values);
            }
        }
    }

    private static void findValues(JSONArray jsonArray, String targetKey, String compareKey, String compareValue, List<String> values) {
        for (int i = 0; i < jsonArray.length(); i++) {
            Object item = jsonArray.get(i);
            if (item instanceof JSONObject) {
                findValues((JSONObject) item, targetKey, compareKey, compareValue, values);
            } else if (item instanceof JSONArray) {
                findValues((JSONArray) item, targetKey, compareKey, compareValue, values);
            }
        }
    }

    public List<String> evaluate(String jsonStr, String targetKey, String compareKey, String compareValue) {
        try {
            List<String> values = new ArrayList<>();
            Object json = new JSONTokener(jsonStr).nextValue();
            if (json instanceof JSONObject) {
                findValues((JSONObject) json, targetKey, compareKey, compareValue, values);
            } else if (json instanceof JSONArray) {
                findValues((JSONArray) json, targetKey, compareKey, compareValue, values);
            }
            return values;
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    public static void main(String[] args) {
        String json = "[{\"sender_type\":\"USER\",\"text\":\"目前，您正在处理一个案例。案例信息如下：\\ncase_title='''我发现我和学生们是真开心 比和大人一起玩开心多了\\uD83E\\uDD23'''\\ncase_content='''我发现我和学生们是真开心 比和大人一起玩开心多了'''。请分析案例并输出结果。直接输出打标结果即可，不需要输出原因\",\"name\":\"用户\"}]";
        String key = "text";
        String key1 = "sender_type";
        String value = "USE";

        get_json_value1 get_json_value = new get_json_value1();

        List list =  get_json_value.evaluate(json,key,key1,value);

        System.out.println(list.toString());


    }

}
