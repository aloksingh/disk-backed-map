package com.alok.diskmap;

import junit.framework.TestCase;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class RBTreeTest extends TestCase{

    public void testSerialization() throws IOException, ClassNotFoundException {
        RBTree rbTree = new RBTree();
        List<Integer> keys = new ArrayList<Integer>();
        for(int i = 0; i < 1000; i++){
            int key = (int) (Math.random() * 10000);
            rbTree.insert(key, (long) (Math.random() * 10000));
            keys.add(key);
        }
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        ObjectOutputStream o = new ObjectOutputStream(buffer);
        o.writeObject(rbTree);
        ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(buffer.toByteArray()));
        RBTree rbTree2 = (RBTree) in.readObject();
        for (Integer key : keys) {
            long[] values = rbTree.lookup(key);
            long[] values2 = rbTree2.lookup(key);
            for (int i = 0; i < values.length; i++) {
                assertEquals(values[i], values2[i]);
            }
        }
    }
}
