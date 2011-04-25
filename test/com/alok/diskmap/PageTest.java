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

import java.io.File;

public class PageTest extends TestCase {
    public void testLookup(){
        Page<String,String> page = new Page<String, String>(new File("/home/alok/sw_dev/tmp"), 1);
        int count = 5000;
        for(int i = 0; i < count; i++){
            page.save("key" + i, "value" + i );
        }
        for(int i = 0; i < count; i++){
            int key = (int)(Math.random() * count);
            String value = page.load("key" + key);
            System.out.println(String.format("Key[%s], Value[%s]", "key" + key, value));
            assertEquals("value" + key,  value);
        }
    }
}
