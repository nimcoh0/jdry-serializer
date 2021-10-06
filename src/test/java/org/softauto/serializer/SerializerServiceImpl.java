package org.softauto.serializer;


import org.softauto.serializer.service.Message;
import org.softauto.serializer.service.SerializerService;


public class SerializerServiceImpl implements SerializerService{
    @Override
    public Object execute(Message message) throws Exception {
        System.out.print(message.getData("data"));
        return "ok";
    }

}
