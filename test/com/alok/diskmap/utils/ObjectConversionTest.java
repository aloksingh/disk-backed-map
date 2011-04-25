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

import com.alok.diskmap.mock.MockSimpleObject;
import junit.framework.TestCase;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class ObjectConversionTest extends TestCase {
    public void testConversionSpeed() throws Exception {
        List<MockSimpleObject> objects = new ArrayList<MockSimpleObject>();
        for(int i = 0; i < 1000; i++){
            objects.add(new MockSimpleObject());
        }
        roundTrip(objects, new Hessian2ObjectConverter());
        roundTrip(objects, new DefaultObjectConverter());
    }

    private void roundTrip(List<MockSimpleObject> objects, ObjectConverter o1) throws Exception {
        List<byte[]> buffers = serialize(objects, o1);
        long size = 0;
        for (byte[] buffer : buffers) {
            size+= buffer.length;
        }
        System.out.println("Size:" + size);
        List<MockSimpleObject> newObjects = deserialize(buffers, o1);
        for (int i = 0; i < objects.size(); i++) {
            assertTrue(objects.get(i).equals(newObjects.get(i)));
        }
    }

    private List<byte[]> serialize(List<MockSimpleObject> objects, ObjectConverter converter) throws Exception {
        List<byte[]> buffers = new ArrayList<byte[]>(objects.size());
        long time = System.currentTimeMillis();
        for (MockSimpleObject object : objects) {
            buffers.add(converter.serialize(object));
        }
        time = System.currentTimeMillis() - time;
        System.out.println(converter.getClass().getName() + " serialization time:" + time);
        return buffers;
    }

    private List<MockSimpleObject> deserialize(List<byte[]> buffers, ObjectConverter converter) throws Exception {
        List<MockSimpleObject> objects = new ArrayList<MockSimpleObject>(buffers.size());
        long time = System.currentTimeMillis();
        for (byte[] buffer : buffers) {
            objects.add(converter.<MockSimpleObject>deserialize(buffer));
        }
        time = System.currentTimeMillis() - time;
        System.out.println(converter.getClass().getName() + " deserialization time:" + time);
        return objects;
    }

    public void testBigDecimal() throws Exception {
        ObjectConverter o1 = new Hessian2ObjectConverter();
        BigDecimal bigDecimal = new BigDecimal("12474639.945458954");
        byte[] bytes = o1.serialize(bigDecimal);
        BigDecimal newDecimal = o1.deserialize(bytes);
        assertEquals(bigDecimal, newDecimal);
    }
}
