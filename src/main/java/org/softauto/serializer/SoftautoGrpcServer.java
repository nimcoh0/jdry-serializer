package org.softauto.serializer;


import io.grpc.MethodDescriptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import org.apache.avro.Protocol;
import org.softauto.serializer.kryo.KryoSerialization;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Provides components to set up a gRPC Server for serialization.
 * Base on Avro
 */
public abstract class SoftautoGrpcServer {


  private static final org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(SoftautoGrpcServer.class);
  static ISerialization serializationEngine = KryoSerialization.getInstance();

  /**
   * Creates a {@link ServerServiceDefinition} for Avro Interface and its
   * implementation that can be passed a gRPC Server.
   *
   * @param iface Avro generated RPC service interface for which service defintion
   *              is created.
   * @return a new server service definition.
   */
  public static ServerServiceDefinition createServiceDefinition(Class iface,Object impl) {
    Protocol protocol = SoftautoGrpcUtils.getProtocol(iface);
    ServiceDescriptor serviceDescriptor = (ServiceDescriptor) ServiceDescriptor.create(iface);
    ServerServiceDefinition.Builder serviceDefinitionBuilder = ServerServiceDefinition
        .builder(serviceDescriptor.getServiceName());
    Map<String, Protocol.Message> messages = protocol.getMessages();
    for (Method method : iface.getMethods()) {
      Protocol.Message msg = messages.get(method.getName());
      // setup a method handler only if corresponding message exists in avro protocol.
      if (msg != null ) {
          try {
            UnaryMethodHandler methodHandler = null;
            methodHandler = msg.isOneWay() ? new OneWayUnaryMethodHandler(method,impl)
                      : new UnaryMethodHandler(method,impl);
            serviceDescriptor.setSerializationEngine(serializationEngine);
            serviceDefinitionBuilder.addMethod(
                    serviceDescriptor.getMethod(method.getName(), MethodDescriptor.MethodType.UNARY),
                    ServerCalls.asyncUnaryCall(methodHandler));
          }catch (Exception e){
            e.printStackTrace();
          }
        }

    }
    return serviceDefinitionBuilder.build();
  }

    public ISerialization getSerializationEngine() {
        return serializationEngine;
    }

    public static void setSerializationEngine(ISerialization serializationEngine) {
        SoftautoGrpcServer.serializationEngine = serializationEngine;
    }


  protected static class UnaryMethodHandler implements ServerCalls.UnaryMethod<Object[], Object> {

      private final Method method;
      private Object impl;

      UnaryMethodHandler(Method method, Object impl) {
          this.method = method;
          this.impl = impl;
      }

      @Override
      public synchronized void invoke(Object[] request, StreamObserver<Object> responseObserver) {
          Object methodResponse = null;
          try {
              //Object serviceImpl = impl.newInstance();
              Method m = Utils.getMethod(impl, method.getName(), method.getParameterTypes());
              if(m == null){
                  logger.error("method is null");
                  throw new Exception("method is null");
              }
              logger.debug("invoking " + method.getName()+ " request "+ Utils.result2String(request));
              m.setAccessible(true);
              if(request == null){
                  logger.error("request is null");
                  throw new Exception("request is null");
              }
              methodResponse = m.invoke(impl, request);
              logger.debug("successfully invoke "+ Arrays.toString(request) + " on "+impl.getClass().getName() );
              logger.debug("got result :"+ methodResponse);
          } catch (InvocationTargetException e) {
              logger.error("fail invoke method " + method, e);
              methodResponse = e.getTargetException();
          } catch (Exception e) {
              logger.error("fail invoke method " + method, e);
              methodResponse = e;
          }
          responseObserver.onNext(methodResponse);
          responseObserver.onCompleted();
      }
  }

      private static class OneWayUnaryMethodHandler extends UnaryMethodHandler {
          private static final Logger LOG = Logger.getLogger(OneWayUnaryMethodHandler.class.getName());
          private Method method;
          private Object impl;


          OneWayUnaryMethodHandler(Method method, Object impl) {
              super(method, impl);
              this.method = method;
              this.impl = impl;
          }


          @Override
          public synchronized void invoke(Object[] request, StreamObserver<Object> responseObserver) {
              responseObserver.onNext(null);
              responseObserver.onCompleted();

              try {
                  //Object serviceImpl = impl.newInstance();
                  Method m = Utils.getMethod(impl, method.getName(), method.getParameterTypes());
                  if(m == null){
                      logger.error("method is null");
                      throw new Exception("method is null");
                  }
                  logger.debug("invoking " + method + " with request "+ Utils.result2String(request));
                  m.setAccessible(true);
                  if(request == null){
                      logger.error("request is null");
                      throw new Exception("request is null");
                  }
                  m.invoke(impl, request);
              } catch (Exception e) {
                  logger.error("fail invoke method " + method, e);
                  Throwable cause = e;
                  while (cause.getCause() != null && cause != cause.getCause()) {
                      cause = cause.getCause();
                  }
                  LOG.log(Level.WARNING, "Error processing one-way rpc", cause);
              }
          }
      }
  }
