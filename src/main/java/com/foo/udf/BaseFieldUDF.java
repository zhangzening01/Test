package com.foo.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author 宁· 2020/3/30
 * 解析基础的字段表
 */
public class BaseFieldUDF extends UDF {
    public String evaluate(String line, String jsonkeysString) {

        // 0 准备一个 sb
        StringBuilder sb = new StringBuilder();
        // 1 切割 jsonkeys mid uid vc vn l sr os ar md
        String[] jsonkeys = jsonkeysString.split(",");
        // 2 处理 line 服务器时间 | json
        String[] logContents = line.split("\\|");
        // 3 合法性校验
        if (logContents.length != 2 || StringUtils.isBlank(logContents[1])) {
            return "";
        }
        // 4 开始处理 json
        try {
            JSONObject jsonObject = new JSONObject(logContents[1]);
            // 获取 cm 里面的对象
            JSONObject base = jsonObject.getJSONObject("cm");
            // 循环遍历取值
            for (int i = 0; i < jsonkeys.length; i++) {
                String filedName = jsonkeys[i].trim();
                if (base.has(filedName)) {
                    sb.append(base.getString(filedName)).append("\t");
                } else {
                    sb.append("\t");
                }
            }
            sb.append(jsonObject.getString("et")).append("\t");
            sb.append(logContents[0]).append("\t");
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }
    public static void main(String[] args) {
        String line ="1585297337786|{\"cm\":{\"ln\":\"-61.1\",\"sv\":\"V2.6.1\",\"os\":\"8.0.0\",\"g\":\"Q094Y5C9@gmail.com\",\"mid\":\"989\",\"nw\":\"WIFI\",\"l\":\"en\",\"vc\":\"12\",\"hw\":\"750*1134\",\"ar\":\"MX\",\"uid\":\"989\",\"t\":\"1585205721904\",\"la\":\"-12.5\",\"md\":\"sumsung-7\",\"vn\":\"1.2.8\",\"ba\":\"Sumsung\",\"sr\":\"A\"},\"ap\":\"app\",\"et\":[{\"ett\":\"1585209987173\",\"en\":\"loading\",\"kv\":{\"extend2\":\"\",\"loading_time\":\"24\",\"action\":\"1\",\"extend1\":\"\",\"type\":\"1\",\"type1\":\"\",\"loading_way\":\"1\"}},{\"ett\":\"1585261672554\",\"en\":\"ad\",\"kv\":{\"entry\":\"1\",\"show_style\":\"2\",\"action\":\"1\",\"detail\":\"433\",\"source\":\"3\",\"behavior\":\"2\",\"content\":\"2\",\"newstype\":\"8\"}},{\"ett\":\"1585275584235\",\"en\":\"notification\",\"kv\":{\"ap_time\":\"1585209679341\",\"action\":\"2\",\"type\":\"3\",\"content\":\"\"}},{\"ett\":\"1585280204422\",\"en\":\"active_foreground\",\"kv\":{\"access\":\"\",\"push_id\":\"1\"}},{\"ett\":\"1585248263760\",\"en\":\"error\",\"kv\":{\"errorDetail\":\"java.lang.NullPointerException\\\\n    at cn.lift.appIn.web.AbstractBaseController.validInbound(AbstractBaseController.java:72)\\\\n at cn.lift.dfdf.web.AbstractBaseController.validInbound\",\"errorBrief\":\"at cn.lift.dfdf.web.AbstractBaseController.validInbound(AbstractBaseController.java:72)\"}},{\"ett\":\"1585289226216\",\"en\":\"favorites\",\"kv\":{\"course_id\":5,\"id\":0,\"add_time\":\"1585239547431\",\"userid\":7}}]}";
        String x = new BaseFieldUDF().evaluate(line, "mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t");
        System.out.println(x);
    }
}
