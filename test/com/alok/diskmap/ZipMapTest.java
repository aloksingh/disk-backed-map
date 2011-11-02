package com.alok.diskmap;

import junit.framework.TestCase;

import java.util.*;

public class ZipMapTest extends TestCase{
    public void testZipMap(){
        Map<String, String> data = new HashMap<String, String>();
        ZipMap zipMap = new ZipMap();
        for(int i = 0; i < 10; i++){
            String key = UUID.randomUUID().toString();
            String value = UUID.randomUUID().toString();
            data.put(key, value);
            zipMap.put(key, value.getBytes());
        }
        for (String key : data.keySet()) {
            assertEquals(data.get(key), new String(zipMap.get(key)));
        }
    }

    public void testMapMemUsage(){
        Map<String, String> data = new HashMap<String, String>();
        int i = 0;
        while(true){
            String key = UUID.randomUUID().toString();
            String value = UUID.randomUUID().toString();
            data.put(key, value);
            i++;
            if(i%10000 == 0){
                System.out.println("Size:" + i);
            }
        }
    }

    public void testZipMapMemUsage(){
        List<ZipMap> maps = new ArrayList<ZipMap>();
        int mapCount = 20000;
        for(int i = 0; i < mapCount; i++){
            maps.add(new ZipMap());
        }
        int i = 0;
        int size = 0;
        while(true){
            String key = UUID.randomUUID().toString();
            String value = UUID.randomUUID().toString();
            maps.get(i% mapCount).put(key, value.getBytes());
            i++;
            size += key.length() + value.length();
            if(i%10000 == 0){
                System.out.println("Count:" + i + ", size:" + size);
            }
        }
    }
}
