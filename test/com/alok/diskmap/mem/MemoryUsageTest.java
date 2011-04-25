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

package com.alok.diskmap.mem;

import com.alok.diskmap.DiskBackedMap;
import junit.framework.TestCase;

import java.util.HashMap;
import java.util.Map;

public class MemoryUsageTest extends TestCase {
    public void testHashMapMemoryUsage(){
        long startMemory = Runtime.getRuntime().totalMemory();
        Map<Integer, String> map = new HashMap<Integer, String>();
        for(int i = 1; i < Integer.MAX_VALUE; i++){
            if(i %10000 == 0){
                System.out.println("Average Used Memory:" + (Runtime.getRuntime().totalMemory())/i);
                System.out.println("entries:" + i);
            }
            map.put(new Integer(i), "Abcdefghijklmnopqrstuvwxyz" + Math.random());
        }
    }

    public void testDiskMapMemoryUsage(){
        long startMemory = Runtime.getRuntime().totalMemory();
        Map<Integer, String> map = new DiskBackedMap<Integer, String>("/tmp/tests");
        for(int i = 1; i < Integer.MAX_VALUE; i++){
            if(i %10000 == 0){
                System.out.println("Average Used Memory:" + (Runtime.getRuntime().totalMemory())/i);
                System.out.println("entries:" + i);
            }
            map.put(new Integer(i), "Abcdefghijklmnopqrstuvwxyz" + Math.random());
        }
    }
}
