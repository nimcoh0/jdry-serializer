package org.softauto.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.junit.Before;
import org.junit.Test;
import org.softauto.grpc.kryo.KryoSerialization;
import org.softauto.grpc.service.SerializerService;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class TestSyncGrpc {

    int port ;


    @Before
    public void setUp() throws IOException {
        SerializerServiceImpl impl = new SerializerServiceImpl(){
            @Override
            public Object execute(String descriptor, Object[] request) throws Exception {
                return "ok";

            }
        };
        setup(SerializerService.class,impl);
    }


    private void setup(Class iface,SerializerServiceImpl impl)throws IOException {
        SoftautoGrpcServer.setSerializationEngine(KryoSerialization.getInstance());
        Server server = ServerBuilder.forPort(0)
                    .addService(SoftautoGrpcServer.createServiceDefinition(iface, impl))
                    .build();
        server.start();
        port = server.getPort();
    }

    @Test
    public void testSync() throws Exception {
        Serializer serializer = new Serializer().setHost("localhost").setPort(port);
        String result = serializer.write("test",new Object[]{TestClass.class});
        assertTrue(result.equals("ok"));
    }



    private class TestClass  {

        public int add(int arg1, int arg2) {
            return 0;
        }
        public ByteBuffer echoBytes(ByteBuffer data) {
            return null;
        }
        public void error()  {}
        public void ping() { }
    }
}
