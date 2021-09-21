## Serialization

jdry-serializer can serialize any type without the need to define special types. 
it make use of [Avro-grpc](https://avro.apache.org/) & [kryo framwork](https://github.com/EsotericSoftware/kryo) for the Serialization .
and support sync & async communication 

> please see [kryo](https://github.com/EsotericSoftware/kryo) for adding  unsupported types



# how to use for sync 

**serialize**
the *serializer.write* method except two arguments descriptor and Object array for the real data 
for example sending message with description "test" and a class as the data .

    Serializer serializer = new Serializer().setHost("localhost").setPort(port);  
    String result = serializer.write("test",new Object[]{TestClass.class});
**deserialize**

you should impl the service interface (SerializerService) method execute to do what ever you want to do with the data

      
    import org.softauto.grpc.service.SerializerService;  
    public class SerializerServiceImpl implements SerializerService{  
      @Override  
      public Object execute(String descriptor, Object[] request) throws Exception {  
            return null;  
      }  
    }

 load the grpc server
 
    Class iface = SerializerService.class
    Object impl = new SerializerServiceImpl();
        Server server = ServerBuilder.forPort(port)  
                    .addService(SoftautoGrpcServer.createServiceDefinition(iface, impl))  
                    .build();  
        server.start();

# how to use for async
**serialize**
the *serializer.write* method except three arguments descriptor , Object array for the real data 
and Callback 
for example sending message with description "test" and a class as the data .

    Serializer serializer = new Serializer().setHost("localhost").setPort(port); 
    CallFuture<String> future = new CallFuture<>(); 
    serializer.write("test",new Object[]{TestClass.class},future);
    

> for the rest of the parts same as sync method


