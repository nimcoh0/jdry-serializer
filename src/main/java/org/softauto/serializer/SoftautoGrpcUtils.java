package org.softauto.serializer;

import io.grpc.KnownLength;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Protocol;

import java.io.InputStream;

/** Utility methods for using Avro IDL and serialization with gRPC. */
public final class SoftautoGrpcUtils {
  private static final org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(SoftautoGrpcUtils.class);
  private SoftautoGrpcUtils() {
  }

  /**
   * Provides a a unique gRPC service name for Avro RPC interface or its subclass
   * Callback Interface.
   *
   * @param iface Avro RPC interface.
   * @return unique service name for gRPC.
   */
  public static String getServiceName(Class iface) {
    Protocol protocol = getProtocol(iface);
    return protocol.getNamespace() + "." + protocol.getName();
  }

  /**
   * Gets the {@link Protocol} from the Avro Interface.
   */
  public static Protocol getProtocol(Class iface) {
    try {
      Protocol p = (Protocol) (iface.getDeclaredField("PROTOCOL").get(null));
      return p;
    } catch (NoSuchFieldException e) {
      throw new AvroRuntimeException("Not a Specific protocol: " + iface);
    } catch (IllegalAccessException e) {
      throw new AvroRuntimeException(e);
    }
  }

  /**
   * Skips any unread bytes from InputStream and closes it.
   */
  static void skipAndCloseQuietly(InputStream stream) {
    try {
      if (stream instanceof KnownLength && stream.available() > 0) {
        stream.skip(stream.available());
      } else {
        // don't expect this for an inputStream provided by gRPC but just to be on safe
        // side.
        byte[] skipBuffer = new byte[4096];
        while (true) {
          int read = stream.read(skipBuffer);
          if (read < skipBuffer.length) {
            break;
          }
        }
      }
      stream.close();
    } catch (Exception e) {
      logger.warn( "failed to skip/close the input stream, may cause memory leak", e);
    }
  }
}
