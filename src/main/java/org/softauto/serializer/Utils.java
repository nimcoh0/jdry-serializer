package org.softauto.serializer;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.lang.reflect.Method;
import java.util.List;

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

    public static String result2String(Object result){
        try{

            if(result != null){
                if(result instanceof List){
                    return ToStringBuilder.reflectionToString(((List)result).toArray(), new MultipleRecursiveToStringStyle());
                }else {
                    return ToStringBuilder.reflectionToString(result, new MultipleRecursiveToStringStyle());
                }
            }
        }catch(Exception e){
            logger.warn("result to String fail on  ",e.getMessage());
        }
        return "";
    }



}
