/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package org.softauto.grpc.service;

import org.apache.avro.Protocol;
import org.softauto.grpc.CallFuture;

@org.apache.avro.specific.AvroGenerated
public interface SerializerService {
  public static final Protocol PROTOCOL = Protocol.parse("{\"protocol\":\"SerializerService\",\"namespace\":\"tests.infrastructure\",\"version\":\"1.0.0\",\"types\":[{\"name\" : \"java.lang.String\",\"type\" : \"external\"},{\"name\" : \"java.lang.Object\",\"type\" : \"external\"},{\"name\" : \"java.lang.Object[]\",\"type\" : \"external\"}],\"messages\":{\"execute\":{\"request\":[{\"name\":\"descriptor\",\"type\":\"java.lang.String\"},{\"name\":\"request\",\"type\":\"java.lang.Object[]\"}],\"response\":{\"type\":\"java.lang.Object\"}}}}");


  /**
   */

  Object execute(String descriptor, Object[] request) throws Exception;

  @SuppressWarnings("all")
  public interface Callback extends SerializerService {
    public static final Protocol PROTOCOL = SerializerService.PROTOCOL;
    /**
     * @throws java.io.IOException The async call could not be completed.
     */

    void execute(String descriptor, Object[] request, CallFuture<Object> callback) throws Exception;
  }
}