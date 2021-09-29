package org.softauto.serializer;

public interface ISerialization {

    public byte[] serialize(Object obj) throws Exception;
    public <T> T deserialize(final byte[] objectData) throws Exception;
    public <T> T deserialize(byte[] objectData, Class<T> type) throws Exception ;

}
