package com.jdbcagent.core.util.serialize;


import com.caucho.hessian.io.Hessian2Input;
import com.caucho.hessian.io.Hessian2Output;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class HessianSerializeUtil {
    public static byte[] serialize(Object obj) {
        if (obj == null)
            return null;
        ByteArrayOutputStream os = null;
        Hessian2Output ho = null;
        try {
            os = new ByteArrayOutputStream();
            ho = new Hessian2Output(os);
            ho.writeObject(obj);
            ho.flush();
            return os.toByteArray();
        } catch (IOException e) {
            return null;
        } finally {
            try {
                if (ho != null) {
                    ho.close();
                    ho = null;
                }
                if (os != null) {
                    os.close();
                    os = null;
                }
            } catch (IOException e) {
                //ignore
            }
        }
    }

    public static Object deserialize(byte[] by) {
        if (by == null)
            return null;
        ByteArrayInputStream is = null;
        Hessian2Input hi = null;
        try {
            is = new ByteArrayInputStream(by);
            hi = new Hessian2Input(is);
            return hi.readObject();
        } catch (IOException e) {
            return null;
        } finally {
            try {
                if (hi != null) {
                    hi.close();
                    hi = null;
                }
                if (is != null) {
                    is.close();
                    is = null;
                }
            } catch (IOException e) {
                //ignore
            }
        }
    }
}
