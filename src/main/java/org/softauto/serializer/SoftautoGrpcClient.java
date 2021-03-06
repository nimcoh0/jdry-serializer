package org.softauto.serializer;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.MethodDescriptor;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import org.apache.avro.AvroRemoteException;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Protocol;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.util.Arrays;


/** Component that sets up a gRPC client for  Serialization.
 * base on avro
 * */
public abstract class SoftautoGrpcClient {

  private static final org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(SoftautoGrpcClient.class);

  private SoftautoGrpcClient() {
  }

  /**
   * Creates a gRPC client for Avro's interface with default {@link CallOptions}.
   *
   * @param channel the channel used for gRPC {@link ClientCalls}.
   * @param iface   Avro interface for which client is built.
   * @param         <T> type of Avro Interface.
   * @return a new client proxy.
   */
  public static <T> T create(Channel channel, Class<T> iface) {
    return create(channel, iface, CallOptions.DEFAULT);
  }

  /**
   * Creates a gRPC client for Avro's interface with provided {@link CallOptions}.
   *
   * @param channel     the channel used for gRPC {@link ClientCalls}.
   * @param iface       Avro interface for which client is built.
   * @param callOptions client call options for gRPC.
   * @param             <T> type of Avro Interface.
   * @return a new client proxy.
   */
  public static <T> T create(Channel channel, Class<T> iface, CallOptions callOptions) {
    Protocol protocol = SoftautoGrpcUtils.getProtocol(iface);
    ServiceDescriptor serviceDescriptor = ServiceDescriptor.create(iface);
    ServiceInvocationHandler proxyHandler = new ServiceInvocationHandler(channel, callOptions, protocol,
        serviceDescriptor,iface);

    return (T) Proxy.newProxyInstance(iface.getClassLoader(), new Class[] { iface }, proxyHandler);

  }

  public static class ServiceInvocationHandler implements InvocationHandler {
    private  Channel channel;
    private  CallOptions callOptions;
    private  ServiceDescriptor serviceDescriptor;
    Class<?> iface ;

    ServiceInvocationHandler(Channel channel, CallOptions callOptions, Protocol protocol,
        ServiceDescriptor serviceDescriptor) {
      this.channel = channel;
      this.callOptions = callOptions;
      this.serviceDescriptor = serviceDescriptor;
    }

    ServiceInvocationHandler(Channel channel, CallOptions callOptions, Protocol protocol,
                             ServiceDescriptor serviceDescriptor, Class<?> iface) {
      this.channel = channel;
      this.callOptions = callOptions;
      this.serviceDescriptor = serviceDescriptor;
      this.iface = iface;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      try {
        logger.debug("invoke method " + method.getName() + " with args " + Utils.result2String(args) );
        return invokeUnaryMethod(method, args);

      } catch (RuntimeException re) {
        logger.error("fail invoke method "+ method.getName() ,re);
        // rethrow any runtime exception
        throw re;
      } catch (Exception e) {
        logger.error("fail invoke method "+ method.getName() ,e);
        throw new AvroRemoteException(e);
      }
    }





    public void setIface(Class<?> iface) {
      this.iface = iface;
    }

    private Object invokeUnaryMethod(Method method, Object[] args) throws Exception {
      Type[] parameterTypes = method.getParameterTypes();
      if ((parameterTypes.length > 0) && (parameterTypes[parameterTypes.length - 1] instanceof Class)
          && Callback.class.isAssignableFrom(((Class<?>) parameterTypes[parameterTypes.length - 1]))) {
        // get the callback argument from the end
        Object[] finalArgs = Arrays.copyOf(args, args.length - 1);
        Callback<?> callback = (Callback<?>) args[args.length - 1];
        unaryRequest(method.getName(), finalArgs, callback);
        return null;
      } else {
        logger.debug("invoke sync method "+method.getName());
        return unaryRequest(method.getName(), args);
      }
    }

    private Object unaryRequest(String methodName, Object[] args) throws Exception {
      logger.debug("invoke async method "+methodName);
      CallFuture<Object> callFuture = new CallFuture<>();
      unaryRequest(methodName, args, callFuture);
      try {
        return callFuture.get();
      } catch (Exception e) {
        if (e.getCause() instanceof Exception) {
          throw (Exception) e.getCause();
        }
        throw new AvroRemoteException(e.getCause());
      }
    }

    private <RespT> void unaryRequest(String methodName, Object[] args, Callback<RespT> callback) throws Exception {
      StreamObserver<Object> observerAdpater = new CallbackToResponseStreamObserverAdpater<>(callback);
      ClientCalls.asyncUnaryCall(
          channel.newCall(serviceDescriptor.getMethod(methodName, MethodDescriptor.MethodType.UNARY), callOptions),
          args, observerAdpater);
    }



    public static class CallbackToResponseStreamObserverAdpater<T> implements StreamObserver<Object> , Serializable {
      private final Callback<T> callback;

      public CallbackToResponseStreamObserverAdpater(Callback<T> callback) {
        this.callback = callback;
      }

      @Override
      public void onNext(Object value) {
        if (value instanceof Throwable) {
          callback.handleError((Throwable) value);
        } else {
          callback.handleResult((T) value);
        }
      }

      @Override
      public void onError(Throwable t) {
        if(t.getMessage().contains("UNAVAILABLE")){
          callback.handleResult(null);
          logger.warn("got communication UNAVAILABLE . test may end");
          return;
        }
        callback.handleError(new AvroRuntimeException(t));
      }

      @Override
      public void onCompleted() {
        // do nothing as there is no equivalent in Callback.
      }
    }
  }
}
