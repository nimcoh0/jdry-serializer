/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.softauto.serializer;


import io.grpc.MethodDescriptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import org.apache.avro.Protocol;
import org.softauto.serializer.kryo.KryoSerialization;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
      private Object serviceImpl;

      UnaryMethodHandler(Method method, Object impl) {
          this.method = method;
          this.serviceImpl = impl;
      }

      @Override
      public void invoke(Object[] request, StreamObserver<Object> responseObserver) {
          Object methodResponse = null;
          try {
              Method m = Utils.getMethod(serviceImpl, method.getName(), method.getParameterTypes());
              logger.debug("invoking " + method);
              m.setAccessible(true);
              methodResponse = m.invoke(serviceImpl, request);
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
          private Object serviceImpl;


          OneWayUnaryMethodHandler(Method method, Object impl) {
              super(method, impl);
              this.method = method;
              this.serviceImpl = impl;
          }


          @Override
          public void invoke(Object[] request, StreamObserver<Object> responseObserver) {
              responseObserver.onNext(null);
              responseObserver.onCompleted();

              try {
                  Method m = Utils.getMethod(serviceImpl, method.getName(), method.getParameterTypes());
                  logger.debug("invoking " + method);
                  m.setAccessible(true);
                  m.invoke(serviceImpl, request);
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
