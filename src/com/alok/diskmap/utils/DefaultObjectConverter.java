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

import java.io.*;

public class DefaultObjectConverter implements ObjectConverter {
    @Override
    public byte[] serialize(Serializable object) throws Exception {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        new ObjectOutputStream(bout).writeObject(object);
        bout.flush();
        return bout.toByteArray();
    }

    @Override
    public <T> T deserialize(byte[] buffer) {
        try {
            ObjectInputStream ios = new ObjectInputStream(new ByteArrayInputStream(buffer));
            return (T) ios.readObject();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
