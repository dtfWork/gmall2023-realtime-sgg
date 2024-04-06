package com.zxkw.flume.interceptor;

import com.zxkw.flume.utils.JSONUtil;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

public class ETLInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    /*
    * 校验json字符串是否完整，不是完整json直接删掉
    * */
    @Override
    public Event intercept(Event event) {
        //1 获取json数据
        byte[] body = event.getBody();
            //把字节数组数据转为UTF_8的字符串
        String log = new String(body, StandardCharsets.UTF_8);

        //2 校验json数据是否是合格的json
        if (JSONUtil.isJSONValidate(log)) {
            //System.out.println("Valid JSON");
            return event;
        } else {
            //System.out.println("Valid JSON");可直接返回全局统一异常,异常处理里可以先保存,再发送异常邮件之类;
            return null;
        }
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        Iterator<Event> iterator = list.iterator();
        while (iterator.hasNext()) {
            Event event = iterator.next();
            if (intercept(event) == null) {
                iterator.remove();
            }
        }
        return list;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new ETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
