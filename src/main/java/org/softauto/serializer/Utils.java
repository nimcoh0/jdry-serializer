package org.softauto.serializer;

import java.lang.reflect.Method;

public class Utils {

    private static final org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(Utils.class);


    public static Method getMethod(Object o, String fullMethodName, Class[] types)throws Exception{
        try {
            Method m = null;
            if(o instanceof Class){
                m = ((Class)o).getDeclaredMethod(getMethodName(fullMethodName), types);
            }else {
                m = o.getClass().getDeclaredMethod(getMethodName(fullMethodName), types);
            }
            m.setAccessible(true);
            return m;
        }catch (Exception e){
            logger.error("fail get method "+ fullMethodName,e);
        }
        return  null;
    }

    public static String getMethodName(String descriptor){
        return descriptor.substring(descriptor.lastIndexOf("_")+1,descriptor.length());
    }





}
