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

package com.alok.diskmap.mock;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class MockSimpleObject implements Serializable {
    private String a;
    private long b;
    private int c;
    private float d;
    private double e;
    private String[] f;
    private List<Long> g;
    private short h;
    private BigInteger i;
    private BigDecimal j;
    public MockSimpleObject(){
        a = UUID.randomUUID().toString();
        b = (long) (1000000 * Math.random());
        c = (int) (1000000 * Math.random());
        d = (float) (1000000f * Math.random());
        e = (1000000 * Math.random());
        f = new String[(int) (1000 * Math.random() + 1)];
        for (int i = 0; i < f.length; i++) {
            f[i] = UUID.randomUUID().toString();
        }
        g = new ArrayList<Long>();
        for (int i = 0; i < f.length; i++) {
            g.add((long) (1000000 * Math.random()));
        }
        h = (short) (10000 * Math.random());
        i = new BigInteger(String.valueOf((long)(1000000 * Math.random())));
        j = new BigDecimal(String.valueOf(100000000 * Math.random()));
    }

    public String getA() {
        return a;
    }

    public void setA(String a) {
        this.a = a;
    }

    public long getB() {
        return b;
    }

    public void setB(long b) {
        this.b = b;
    }

    public int getC() {
        return c;
    }

    public void setC(int c) {
        this.c = c;
    }

    public float getD() {
        return d;
    }

    public void setD(float d) {
        this.d = d;
    }

    public double getE() {
        return e;
    }

    public void setE(double e) {
        this.e = e;
    }

    public String[] getF() {
        return f;
    }

    public void setF(String[] f) {
        this.f = f;
    }

    public List<Long> getG() {
        return g;
    }

    public void setG(List<Long> g) {
        this.g = g;
    }

    public short getH() {
        return h;
    }

    public void setH(short h) {
        this.h = h;
    }

    public BigInteger getI() {
        return i;
    }

    public void setI(BigInteger i) {
        this.i = i;
    }

    public BigDecimal getJ() {
        return j;
    }

    public void setJ(BigDecimal j) {
        this.j = j;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MockSimpleObject that = (MockSimpleObject) o;

        if (b != that.b) {
            return false;
        }
        if (c != that.c) {
            return false;
        }
        if (Float.compare(that.d, d) != 0) {
            return false;
        }
        if (Double.compare(that.e, e) != 0) {
            return false;
        }
        if (h != that.h) {
            return false;
        }
        if (a != null ? !a.equals(that.a) : that.a != null) {
            return false;
        }
        if (!Arrays.equals(f, that.f)) {
            return false;
        }
        if (g != null ? !g.equals(that.g) : that.g != null) {
            return false;
        }
        if (i != null ? !i.equals(that.i) : that.i != null) {
            return false;
        }
        if (j != null ? (j.compareTo(that.j) != 0 ): that.j != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = a != null ? a.hashCode() : 0;
        result = 31 * result + (int) (b ^ (b >>> 32));
        result = 31 * result + c;
        result = 31 * result + (d != +0.0f ? Float.floatToIntBits(d) : 0);
        temp = e != +0.0d ? Double.doubleToLongBits(e) : 0L;
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (f != null ? Arrays.hashCode(f) : 0);
        result = 31 * result + (g != null ? g.hashCode() : 0);
        result = 31 * result + (int) h;
        result = 31 * result + (i != null ? i.hashCode() : 0);
        result = 31 * result + (j != null ? j.hashCode() : 0);
        return result;
    }
}
