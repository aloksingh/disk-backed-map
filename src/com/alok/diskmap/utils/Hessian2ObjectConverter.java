/*
 * Copyright 2009 Alok Singh
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.alok.diskmap.utils;

import com.caucho.hessian.io.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;

public class Hessian2ObjectConverter implements ObjectConverter {
    private Hessian2Output os;
    private SerializerFactory factory;

    public Hessian2ObjectConverter() {
        this.os = new Hessian2Output(null);
        this.factory = new SerializerFactory();
        factory.addFactory(new AbstractSerializerFactory(){
            @Override
            public Serializer getSerializer(Class cl) throws HessianProtocolException {
                if(cl.isAssignableFrom(BigDecimal.class)){
                    return StringValueSerializer.SER;
                }
                return null;
            }

            @Override
            public Deserializer getDeserializer(Class cl) throws HessianProtocolException {
                if(cl.isAssignableFrom(BigDecimal.class)){
                    return new StringValueDeserializer(BigDecimal.class);
                }
                return null;
            }
        });
        this.os.setSerializerFactory(factory);


    }

    @Override
    public byte[] serialize(Serializable object) throws Exception {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        synchronized (os) {
            os.init(buffer);
            os.writeObject(object);
            os.close();
        }
        return buffer.toByteArray();
    }

    @Override
    public <T> T deserialize(byte[] buffer) {
        T v;
        try {
            ByteArrayInputStream stream = new ByteArrayInputStream(buffer);
            Hessian2Input is = new Hessian2Input(stream);
            is.setSerializerFactory(factory);
            v = (T) is.readObject();
            is.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return v;
    }
}
