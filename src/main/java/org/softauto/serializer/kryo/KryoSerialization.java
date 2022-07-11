package org.softauto.serializer.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;
//import com.sun.xml.internal.ws.encoding.soap.SerializationException;
import de.javakaffee.kryoserializers.*;
import de.javakaffee.kryoserializers.guava.*;
import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer;
import de.javakaffee.kryoserializers.jodatime.JodaLocalDateSerializer;
import de.javakaffee.kryoserializers.jodatime.JodaLocalDateTimeSerializer;
import de.javakaffee.kryoserializers.jodatime.JodaLocalTimeSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.softauto.serializer.ISerialization;
import org.softauto.serializer.Serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.InvocationHandler;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.GregorianCalendar;



/**
 * wrapper to Kryo Serialization
 */
public class KryoSerialization implements ISerialization {


    public  Kryo kryo = null;
    private static KryoSerialization kryoSerialization;

    public static KryoSerialization getInstance(){
        if(kryoSerialization == null){
            kryoSerialization = new KryoSerialization();
        }
        return kryoSerialization;
    }

    private KryoSerialization(){
        kryo = new Kryo();
        kryo.setRegistrationRequired(false);
        kryo.setReferences(false);
        kryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
        addSerializers();
    }

    public Kryo getKryo(){
        return kryo;
    }


    public synchronized byte[] serialize(Object obj) throws Exception{
        Output output = null;
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(1024);
            //output = new Output(outputStream,4096);
            output = new Output(1024, -1);
            //output.writeString(obj.toString());
            serialize(obj, output);
            byte[] data =  output.toBytes();
            return data;

        }finally {
            if(output != null){
                output.flush();
                output.close();
            }
        }

    }


    protected synchronized void serialize(Object obj, Output outputStream) {
        if (outputStream == null) {
            throw new IllegalArgumentException("The OutputStream must not be null");
        } else {
            kryo.writeClassAndObject(outputStream, obj);

        }
    }


    public synchronized <T> T deserialize(final byte[] objectData) throws Exception{
        Input input = null;
        Object object = null ;
        try {
            if (objectData == null) {
                throw new IllegalArgumentException("The byte[] must not be null");
            }
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(objectData);
            input = new Input(byteArrayInputStream);
            object = deserialize(input);
            //return (T) object;
        }catch (Throwable e){
            throw new Exception("fail deserialize ",e);
        } finally {
            if (input != null) {
                input.close();
            }
        }
        return (T) object;
    }


    public synchronized <T> T deserialize(byte[] objectData, Class<T> type) throws Exception {
        Input input = null;
        Object object = null ;
        try {
            if (objectData == null) {
                throw new IllegalArgumentException("The byte[] must not be null");
            }
            input = new Input(objectData);
            object = deserialize(input, type);
            //return (T) object;
        }catch (Throwable e){
            throw new Exception("fail deserialize ",e);
        } finally {
            if (input != null) {
                input.close();
            }
        }
        return (T) object;
    }

    protected synchronized <T> T deserialize(Input  inputStream) throws Exception {
        if (inputStream == null) {
            throw new IllegalArgumentException("The InputStream must not be null");
        }
        try{
            return (T)kryo.readClassAndObject(inputStream);

        } catch (Throwable ex) {
            throw new Exception(ex);
        }
        }

    protected synchronized <T> T deserialize(Input  inputStream,Class<T> type) throws Exception {
        if (inputStream == null) {
            throw new IllegalArgumentException("The InputStream must not be null");
        }
        try{
            return (T)kryo.readObject(inputStream,type);

        } catch (Throwable ex) {
            throw new Exception(ex);
        }
    }

    private void addSerializers(){
        kryo.register( Arrays.asList( "" ).getClass(), new ArraysAsListSerializer() );
        kryo.register( Collections.EMPTY_LIST.getClass(), new CollectionsEmptyListSerializer() );
        kryo.register( Collections.EMPTY_MAP.getClass(), new CollectionsEmptyMapSerializer() );
        kryo.register( Collections.EMPTY_SET.getClass(), new CollectionsEmptySetSerializer() );
        kryo.register( Collections.singletonList( "" ).getClass(), new CollectionsSingletonListSerializer() );
        kryo.register( Collections.singleton( "" ).getClass(), new CollectionsSingletonSetSerializer() );
        kryo.register( Collections.singletonMap( "", "" ).getClass(), new CollectionsSingletonMapSerializer() );
        kryo.register( GregorianCalendar.class, new GregorianCalendarSerializer() );
        kryo.register( InvocationHandler.class, new JdkProxySerializer() );
        UnmodifiableCollectionsSerializer.registerSerializers( kryo );
        SynchronizedCollectionsSerializer.registerSerializers( kryo );
        kryo.register( DateTime.class, new JodaDateTimeSerializer() );
        kryo.register( LocalDate.class, new JodaLocalDateSerializer() );
        kryo.register( LocalDateTime.class, new JodaLocalDateTimeSerializer() );
        kryo.register( LocalDateTime.class, new JodaLocalTimeSerializer() );
        ArrayListMultimapSerializer.registerSerializers( kryo );
        HashMultimapSerializer.registerSerializers( kryo );
        LinkedHashMultimapSerializer.registerSerializers( kryo );
        LinkedListMultimapSerializer.registerSerializers( kryo );
        TreeMultimapSerializer.registerSerializers( kryo );
        ArrayTableSerializer.registerSerializers( kryo );
        HashBasedTableSerializer.registerSerializers( kryo );
        TreeBasedTableSerializer.registerSerializers( kryo );

    }

}
