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

public class ConversionUtilsTest extends TestCase{
    private ConversionUtils util;
    public void setUp(){
        this.util = new ConversionUtils();
    }

    public void testIntConversion(){
        for(int i = 0; i < 1000000; i++){
            int k = (int) (Math.random() * Integer.MAX_VALUE);
            int j = (int) (Math.random() * Integer.MIN_VALUE);
            assertEquals(k, util.byteToInt(util.intToBytes(k)));
            assertEquals(j, util.byteToInt(util.intToBytes(j)));
            assertEquals(i, util.byteToInt(util.intToBytes(i)));
        }
    }

    public void testLongConversion(){
        for(long i = 0; i < 100000; i++){
            long k = (long) (Math.random() * Long.MAX_VALUE);
            long j = (long) (Math.random() * Long.MIN_VALUE);
            assertEquals(k, util.byteToLong(util.longToBytes(k)));
            assertEquals(j, util.byteToLong(util.longToBytes(j)));
            assertEquals(i, util.byteToLong(util.longToBytes(i)));
        }
    }
}
