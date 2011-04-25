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

public class Node {
    public int key;
    private long value;
    private long[] values;
    public Node left;
    public Node right;
    public Node parent;
    public int color;

    public Node() {
    }

    public Node(int key, long value, int color, Node left, Node right) {
        this.key = key;
        this.value = value;
        this.color = color;
        this.left = left;
        this.right = right;
    }

    public Node grandparent() {
        assert parent != null; // Not the root node
        assert parent.parent != null; // Not child of root
        return parent.parent;
    }

    public Node sibling() {
        assert parent != null; // Root node has no sibling
        if (this == parent.left)
            return parent.right;
        else
            return parent.left;
    }

    public Node uncle() {
        assert parent != null; // Root node has no uncle
        assert parent.parent != null; // Children of root have no uncle
        return parent.sibling();
    }

    public long getValue() {
        return value;
    }

    public long[] getValues() {
        return values;
    }

    void addValue(long value) {
        if (values == null) {
            values = new long[]{value};
        } else {
            long[] temp = new long[values.length + 1];
            System.arraycopy(values, 0, temp, 0, values.length);
            temp[temp.length - 1] = value;
            values = temp;
        }
    }

    void deleteValue(long value) {
        int idx = -1;
        for (int i = 0; i < values.length; i++) {
            if (values[i] == value) {
                idx = i;
                break;
            }
        }
        if (idx == -1) {
            return;
        }
        long[] temp = new long[values.length - 1];
        if (idx == 0) {
            System.arraycopy(values, 1, temp, 0, values.length - 1);
        } else if (idx == temp.length - 1) {
            System.arraycopy(values, 0, temp, 0, values.length - 1);
        } else {
            System.arraycopy(values, 0, temp, 0, idx);
            System.arraycopy(values, idx + 1, temp, idx, values.length - (idx + 1));
        }
        values = temp;
    }

    void setValue(long value) {
        this.value = value;
    }

    void setValues(long[] values) {
        this.values = new long[values.length];
        System.arraycopy(this.values, 0, values, 0, values.length);
    }


    public static interface Color {
        public static final int RED = 1;
        public static final int BLACK = 2;
    }
}
