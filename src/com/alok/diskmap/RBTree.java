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

import com.alok.diskmap.Node.Color;

public class RBTree {
    private static final int INDENT_STEP = 4;

    public Node root;

    public RBTree() {
        root = null;
    }

    private static int nodeColor(Node n) {
        return n == null ? Color.BLACK : n.color;
    }

    private Node lookupNode(int key) {
        Node n = root;
        while (n != null) {
            int compResult = compare(key, n.key);
            if (compResult == 0) {
                return n;
            } else if (compResult < 0) {
                n = n.left;
            } else {
                assert compResult > 0;
                n = n.right;
            }
        }
        return n;
    }

    private static int compare(int n1, int n2) {
        return (n1 < n2 ? -1 : (n1 == n2 ? 0 : 1));
    }

    public long[] lookup(int key) {
        return _lookup(rehash(key));
    }

    private long[] _lookup(int key) {
        Node n = lookupNode(key);
        if (n == null) {
            return null;
        }
        if (n.getValues() != null) {
            return n.getValues();
        }
        return new long[]{n.getValue()};
    }

    private void rotateLeft(Node n) {
        Node r = n.right;
        replaceNode(n, r);
        n.right = r.left;
        if (r.left != null) {
            r.left.parent = n;
        }
        r.left = n;
        n.parent = r;
    }

    private void rotateRight(Node n) {
        Node l = n.left;
        replaceNode(n, l);
        n.left = l.right;
        if (l.right != null) {
            l.right.parent = n;
        }
        l.right = n;
        n.parent = l;
    }

    private void replaceNode(Node oldn, Node newn) {
        if (oldn.parent == null) {
            root = newn;
        } else {
            if (oldn == oldn.parent.left)
                oldn.parent.left = newn;
            else
                oldn.parent.right = newn;
        }
        if (newn != null) {
            newn.parent = oldn.parent;
        }
    }

    public void insert(int key, long value) {
        _insert(rehash(key), value);
    }

    private void _insert(int key, long value) {
        Node insertedNode = new Node(key, value, Color.RED, null, null);
        if (root == null) {
            root = insertedNode;
        } else {
            Node n = root;
            while (true) {
                int compResult = compare(key, n.key);
                if (compResult == 0) {
                    if (n.getValue() == value) {
                        return;
                    } else if (n.getValues() != null) {
                        n.addValue(value);
                    } else {
                        //hash collision
                        n.addValue(n.getValue());
                        n.addValue(value);
                        n.setValue(-1);
                    }
                    return;
                } else if (compResult < 0) {
                    if (n.left == null) {
                        n.left = insertedNode;
                        break;
                    } else {
                        n = n.left;
                    }
                } else {
                    assert compResult > 0;
                    if (n.right == null) {
                        n.right = insertedNode;
                        break;
                    } else {
                        n = n.right;
                    }
                }
            }
            insertedNode.parent = n;
        }
        insertCase1(insertedNode);
    }

    private void insertCase1(Node n) {
        if (n.parent == null)
            n.color = Color.BLACK;
        else
            insertCase2(n);
    }

    private void insertCase2(Node n) {
        if (nodeColor(n.parent) == Color.BLACK)
            return; // Tree is still valid
        else
            insertCase3(n);
    }

    void insertCase3(Node n) {
        if (nodeColor(n.uncle()) == Color.RED) {
            n.parent.color = Color.BLACK;
            n.uncle().color = Color.BLACK;
            n.grandparent().color = Color.RED;
            insertCase1(n.grandparent());
        } else {
            insertCase4(n);
        }
    }

    void insertCase4(Node n) {
        if (n == n.parent.right && n.parent == n.grandparent().left) {
            rotateLeft(n.parent);
            n = n.left;
        } else if (n == n.parent.left && n.parent == n.grandparent().right) {
            rotateRight(n.parent);
            n = n.right;
        }
        insertCase5(n);
    }

    void insertCase5(Node n) {
        n.parent.color = Color.BLACK;
        n.grandparent().color = Color.RED;
        if (n == n.parent.left && n.parent == n.grandparent().left) {
            rotateRight(n.grandparent());
        } else {
            assert n == n.parent.right && n.parent == n.grandparent().right;
            rotateLeft(n.grandparent());
        }
    }

    public void delete(int key, long value) {
        _delete(rehash(key), value);
    }

    private void _delete(int key, long value) {
        Node n = lookupNode(key);
        if (n == null)
            return;  // Key not found, do nothing
        if (n.getValues() != null) {
            n.deleteValue(value);
            return;
        }
        if (n.left != null && n.right != null) {
            // Copy key/value from predecessor and then delete it instead
            Node pred = maximumNode(n.left);
            n.key = pred.key;
            if (pred.getValues() == null) {
                n.setValue(pred.getValue());
            } else {
                n.setValues(pred.getValues());
            }
            n = pred;
        }

        assert n.left == null || n.right == null;
        Node child = (n.right == null) ? n.left : n.right;
        if (nodeColor(n) == Color.BLACK) {
            n.color = nodeColor(child);
            deleteCase1(n);
        }
        replaceNode(n, child);
    }

    private static Node maximumNode(Node n) {
        assert n != null;
        while (n.right != null) {
            n = n.right;
        }
        return n;
    }

    private void deleteCase1(Node n) {
        if (n.parent == null)
            return;
        else
            deleteCase2(n);
    }

    private void deleteCase2(Node n) {
        if (nodeColor(n.sibling()) == Color.RED) {
            n.parent.color = Color.RED;
            n.sibling().color = Color.BLACK;
            if (n == n.parent.left)
                rotateLeft(n.parent);
            else
                rotateRight(n.parent);
        }
        deleteCase3(n);
    }

    private void deleteCase3(Node n) {
        if (nodeColor(n.parent) == Color.BLACK &&
                nodeColor(n.sibling()) == Color.BLACK &&
                nodeColor(n.sibling().left) == Color.BLACK &&
                nodeColor(n.sibling().right) == Color.BLACK) {
            n.sibling().color = Color.RED;
            deleteCase1(n.parent);
        } else
            deleteCase4(n);
    }

    private void deleteCase4(Node n) {
        if (nodeColor(n.parent) == Color.RED &&
                nodeColor(n.sibling()) == Color.BLACK &&
                nodeColor(n.sibling().left) == Color.BLACK &&
                nodeColor(n.sibling().right) == Color.BLACK) {
            n.sibling().color = Color.RED;
            n.parent.color = Color.BLACK;
        } else
            deleteCase5(n);
    }

    private void deleteCase5(Node n) {
        if (n == n.parent.left &&
                nodeColor(n.sibling()) == Color.BLACK &&
                nodeColor(n.sibling().left) == Color.RED &&
                nodeColor(n.sibling().right) == Color.BLACK) {
            n.sibling().color = Color.RED;
            n.sibling().left.color = Color.BLACK;
            rotateRight(n.sibling());
        } else if (n == n.parent.right &&
                nodeColor(n.sibling()) == Color.BLACK &&
                nodeColor(n.sibling().right) == Color.RED &&
                nodeColor(n.sibling().left) == Color.BLACK) {
            n.sibling().color = Color.RED;
            n.sibling().right.color = Color.BLACK;
            rotateLeft(n.sibling());
        }
        deleteCase6(n);
    }

    private void deleteCase6(Node n) {
        n.sibling().color = nodeColor(n.parent);
        n.parent.color = Color.BLACK;
        if (n == n.parent.left) {
            assert nodeColor(n.sibling().right) == Color.RED;
            n.sibling().right.color = Color.BLACK;
            rotateLeft(n.parent);
        } else {
            assert nodeColor(n.sibling().left) == Color.RED;
            n.sibling().left.color = Color.BLACK;
            rotateRight(n.parent);
        }
    }

    public void print() {
        printHelper(root, 0);
    }

    private static void printHelper(Node n, int indent) {
        if (n == null) {
            System.out.print("<empty tree>");
            return;
        }
        if (n.right != null) {
            printHelper(n.right, indent + INDENT_STEP);
        }
        for (int i = 0; i < indent; i++)
            System.out.print(" ");
        if (n.color == Color.BLACK)
            System.out.println(n.key);
        else
            System.out.println("<" + n.key + ">");
        if (n.left != null) {
            printHelper(n.left, indent + INDENT_STEP);
        }
    }

    public int count() {
        final int[] counter = new int[1];
        counter[0] = 0;

        Visitor visitor = new Visitor() {
            public void visit(Node node) {
                counter[0] =  counter[0] + 1;
                if (node.left != null) {
                    visit(node.left);
                }
                if (node.right != null) {
                    visit(node.right);
                }
            }
        };
        visitor.visit(root);
        return counter[0];
    }

    public static void main(String[] args) {
        RBTree t = new RBTree();
        t.print();

        java.util.Random gen = new java.util.Random();

        int dups = 0;
        for (int i = 0; i < 5000; i++) {
            int x = gen.nextInt(10000);
            long y = gen.nextInt(10000);

//            t.print();
            System.out.print("" + x + " -> " + y + ",");
            System.out.println();
            if(t.lookup(x) != null){
                dups++;
            }
            t.insert(x, y);
            assert t.lookup(x)[0] == y;
        }
        System.out.print(String.format("Expected:%d, Actual: %d ", 5000 -dups, t.count()));
        for (int i = 0; i < 60000; i++) {
            int x = gen.nextInt(10000);

//            t.print();
            System.out.print("Deleting key " + x);
            if (t.lookup(x) != null) {
                t.delete(x, t.lookup(x)[0]);
            }
        }
    }

    private int rehash(int h) {
        h ^= (h >>> 20) ^ (h >>> 12);
        return h ^ (h >>> 7) ^ (h >>> 4);
    }

    public interface Visitor {
        void visit(Node node);
    }
}
