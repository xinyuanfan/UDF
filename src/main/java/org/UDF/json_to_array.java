package org.UDF;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONObject;
import java.util.ArrayList;
import java.util.List;


public class json_to_array extends UDF {


    public List<JSONObject> evaluate(String jsonStr) {
        List<JSONObject> values = new ArrayList<>();
        JSONObject jsonObject = new JSONObject(jsonStr);

        jsonObject.keys().forEachRemaining(key -> System.out.println(key));

        return values;

    }
    public static void main(String[] args) {

        String json = "{\"is_follow\":0,\"is_chat\":0,\"is_chat5\":0,\"is_chat20\":0,\"chat_round\":0,\"normal_chat_round\":0,\"share_popup_view_cnt\":0,\"share_popup_click_cnt\":0,\"is_pay\":0,\"is_draw_card\":0,\"duration\":8.107,\"is_into_detail_page\":0,\"has_voice_click\":0}";

        JSONObject jsonObject = new JSONObject(json);

        jsonObject.keys().forEachRemaining(key -> {
            System.out.println(key);
        });
//        json_to_array jsonToArray = new json_to_array();
//
//        jsonToArray.evaluate(json);


    }
}