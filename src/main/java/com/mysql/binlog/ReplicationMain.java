package com.mysql.binlog;

import cn.hutool.json.JSON;
import cn.hutool.json.JSONObject;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;

import java.io.IOException;


public class ReplicationMain {

    public static void main(String[] args) throws  IOException {

        BinaryLogClient client = new BinaryLogClient("localhost", 3306, "root", "eS4w8aSaxFYoTqZ2");
        EventDeserializer eventDeserializer = new EventDeserializer();
        //时间反序列化的格式
//        eventDeserializer.setCompatibilityMode(
//                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
//                EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
//        );
        client.setEventDeserializer(eventDeserializer);


        client.registerEventListener(new BinaryLogClient.EventListener() {

            @Override
            public void onEvent(Event event) {

                EventHeader header = event.getHeader();


                EventType eventType = header.getEventType();
                System.out.println("监听的事件类型:" + eventType);
//                System.out.println(event.getData());
//                System.out.println(new JSONObject(event.getData()));
                if (EventType.isWrite(eventType)) {
                    //获取事件体
                    WriteRowsEventData data = event.getData();
                    System.out.println(new JSONObject(data));
                } else if (EventType.isUpdate(eventType)) {
                    UpdateRowsEventData data = event.getData();
                    System.out.println(new JSONObject(data));
                } else if (EventType.isDelete(eventType)) {
                    DeleteRowsEventData data = event.getData();
                    System.out.println(new JSONObject(data));
                }
            }
        });
        client.connect();
    }

}
