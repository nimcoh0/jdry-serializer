package org.softauto.serializer;

import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.softauto.serializer.service.Message;
import org.softauto.serializer.service.SerializerService;

public class Serializer {

    private static final Logger logger = LogManager.getLogger(Serializer.class);
    String host;
    int port;
    ManagedChannel channel = null;
    SerializerService client = null;


    public String getHost() {
        return host;
    }

    public Serializer setHost(String host) {
        this.host = host;
        return this;
    }

    public int getPort() {
        return port;
    }

    public Serializer setPort(int port) {
        this.port = port;
        return this;
    }

    public ManagedChannel getChannel() {
        return channel;
    }

    public Serializer setChannel(ManagedChannel channel) {
        this.channel = channel;
        this.client = (SerializerService)SoftautoGrpcClient.create(this.channel, SerializerService.class);
        return this;
    }

    public Serializer build(){
        channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        client = SoftautoGrpcClient.create(channel, SerializerService.class);
        return this;
    }

    public <T> T write(Message message) throws Exception {
        Object result = null;
        try {
            result =  client.execute(message);
            logger.debug("successfully execute message "+ message.toJson());
        }catch (Exception e){
            logger.error("fail execute sync message "+ message.toJson(),e);
        }finally {
            channel.shutdown();
        }
        return (T)result;
    }


    public <T>void write(Message message,  CallFuture<T> callback) throws Exception {
        try {
            //SerializerService.Callback client = SoftautoGrpcClient.create(channel, SerializerService.Callback.class);
            MethodDescriptor<Object[], Object> m = ServiceDescriptor.create(SerializerService.class).getMethod("execute", MethodDescriptor.MethodType.UNARY);
            StreamObserver<Object> observerAdpater = new CallbackToResponseStreamObserverAdpater<>(callback, channel);
            ClientCalls.asyncUnaryCall(channel.newCall(m, CallOptions.DEFAULT), new Object[]{message}, observerAdpater);
        }catch (Exception e){
            logger.error("fail execute async  message "+message.toJson(),e);
        }
    }
}
