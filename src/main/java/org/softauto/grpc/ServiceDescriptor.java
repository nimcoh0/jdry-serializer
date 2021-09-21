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

package org.softauto.grpc;

import io.grpc.MethodDescriptor;
import org.apache.avro.Protocol;
import org.softauto.grpc.kryo.KryoSerialization;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static io.grpc.MethodDescriptor.generateFullMethodName;


/** Descriptor for a gRPC service based on a Avro interface. */
public class ServiceDescriptor  {
  private static final org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(ServiceDescriptor.class);
  protected static final ConcurrentMap<String, ServiceDescriptor> SERVICE_DESCRIPTORS = new ConcurrentHashMap<>();
  protected final ConcurrentMap<String, MethodDescriptor<Object[], Object>> methods = new ConcurrentHashMap<>();
  protected Protocol protocol;
  protected  String serviceName;
  ISerialization serializationEngine = KryoSerialization.getInstance();

  public void setSerializationEngine(ISerialization serializationEngine) {
    this.serializationEngine = serializationEngine;
  }

  private ServiceDescriptor(Class iface, String serviceName) {
    this.serviceName = serviceName;
    this.protocol = SoftautoGrpcUtils.getProtocol(iface);
    SoftautoRequestMarshaller.setSerializer(serializationEngine);
    SoftautoResponseMarshaller.setSerializer(serializationEngine);
  }


  public String getServiceName() {
    return serviceName;
  }

  public static ServiceDescriptor create(Class iface) {
    String serviceName = SoftautoGrpcUtils.getServiceName(iface);
    return SERVICE_DESCRIPTORS.computeIfAbsent(serviceName, key -> new ServiceDescriptor(iface,serviceName));
  }

  /**
   * Provides a gRPC {@link MethodDescriptor} for a RPC method/message of Avro
   * {@link Protocol}.
   *
   * @param methodType gRPC type for the method.
   * @return a {@link MethodDescriptor}
   */

  public MethodDescriptor<Object[], Object> getMethod(String methodName, MethodDescriptor.MethodType methodType) {
    return methods.computeIfAbsent(methodName,
            key -> MethodDescriptor.<Object[], Object>newBuilder()
                    .setFullMethodName(generateFullMethodName(serviceName, methodName)).setType(methodType)
                    .setRequestMarshaller(new SoftautoRequestMarshaller(protocol.getMessages().get(methodName)))
                    .setResponseMarshaller(new SoftautoResponseMarshaller(protocol.getMessages().get(methodName))).build());
  }
}
