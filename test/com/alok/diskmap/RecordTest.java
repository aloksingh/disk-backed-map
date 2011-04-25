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

package com.alok.diskmap;

import junit.framework.TestCase;

import java.io.*;

public class RecordTest extends TestCase{
    public void testReadWrite() throws IOException{
        String key = "foo";
        String value = "bar";
        Record r1 = new Record(key.getBytes(), value.getBytes(), Record.ACTIVE, key.hashCode(), 0);
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(buffer);
        r1.write(out);
        buffer.close();
        Record r2 = new Record();
        DataInput in = new DataInputStream(new ByteArrayInputStream(buffer.toByteArray()));
        r2.read(in);
        assertEquals(r1, r2);
    }
}
