/*
 * Copyright 2011-2015 The Trustees of the University of Pennsylvania
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.upenn.library.xmlaminar.parallel;

import java.nio.BufferOverflowException;

/**
 *
 * @author magibney
 */
public class CircularCharBuffer {
    
    private static final int DEFAULT_INITIAL_CAPACITY = 2048;
    public static final int UNBOUNDED = -1;
    private final int maxCapacity;
    private char[] buf;
    private int head = 0;
    private int tail = 0;
    private int headPosition = 0;
    private int tailPosition = 0;
    private int positionOffset = 0;
    
    private void growBuffer() {
        if (buf.length == maxCapacity) {
            throw new BufferOverflowException();
        }
        char[] old = buf;
        buf = new char[old.length * 2];
        if (tail >= head) {
            System.arraycopy(old, head, buf, 0, tail - head);
            tail -= head;
        } else {
            int len = old.length - head;
            System.arraycopy(old, head, buf, 0, len);
            System.arraycopy(old, 0, buf, len, tail);
            tail += len;
        }
        head = 0;
        int newLen = buf.length;
        positionOffset = -(((headPosition % newLen) + newLen) % newLen);
    }

    public static boolean inOrder(int first, int second) {
        return second - first >= 0;
    }
    
    public void reset() {
        headPosition = 0;
        tailPosition = 0;
        positionOffset = 0;
        head = 0;
        tail = 0;
    }
    
    public int size() {
        int len = buf.length;
        return (((tail - head) % len) + len) % len;
    }
    
    private static int positiveInteger(int val) {
        if (val < 1) {
            throw new IllegalArgumentException(val+" must be > 1");
        }
        return val;
    }
    
    private static int powerOfTwo(int val) {
        if (Integer.bitCount(val) == 1) {
            return val;
        } else {
            return Integer.highestOneBit(val) * 2;
        }
    }
    
    public CircularCharBuffer(int maxCapacity, int initialCapacity) {
        buf = new char[powerOfTwo(positiveInteger(initialCapacity))];
        if (maxCapacity == UNBOUNDED) {
            this.maxCapacity = UNBOUNDED;
        } else if (maxCapacity <= 0) {
            throw new IllegalArgumentException("maxCapacity must be "+UNBOUNDED+" (unbounded), or a positive integer");
        } else {
            this.maxCapacity = powerOfTwo(maxCapacity);
        }
    }
    
    public CircularCharBuffer(int maxCapacity) {
        this(maxCapacity, DEFAULT_INITIAL_CAPACITY);
    }
    
    public CircularCharBuffer() {
        this(UNBOUNDED);
    }
    
    public void write(char[] cbuf, int off, int len) {
        ensureSpace(len);
        int limit;
        if (tail < head || (limit = buf.length - tail) >= len) {
            System.arraycopy(cbuf, off, buf, tail, len);
            tail += len;
            tailPosition += len;
        } else {
            System.arraycopy(cbuf, off, buf, tail, limit);
            int remainder = len - limit;
            System.arraycopy(cbuf, off + limit, buf, 0, remainder);
            tail = remainder;
            tailPosition += (limit + remainder);
        }
    }
    
    public int getPosition() {
        return tailPosition;
    }

    public int read(char[] cbuf, int off, int len, int srcPosition) {
        verifyIdInBounds(srcPosition);
        int blen = buf.length;
        srcPosition = (((srcPosition + positionOffset) % blen) + blen) % blen;
        int limit;
        if (srcPosition == tail) {
            return -1;
        } else if (srcPosition < tail) {
            len = Math.min(len, tail - srcPosition);
            System.arraycopy(buf, srcPosition, cbuf, off, len);
            return len;
        } else if ((limit = buf.length - srcPosition) >= len) {
            System.arraycopy(buf, srcPosition, cbuf, off, len);
            return len;
        } else {
            System.arraycopy(buf, srcPosition, cbuf, off, limit);
            len = Math.min(len - limit, tail);
            System.arraycopy(buf, 0, cbuf, off + limit, len);
            return len + limit;
        }
    }
    
    private void verifyIdInBounds(int id) {
        if (headPosition < tailPosition ? id < headPosition || id > tailPosition : id < headPosition && id > tailPosition) {
            throw new IllegalStateException();
        }
    }
    
    public void trim(int position) {
        verifyIdInBounds(position);
        int len = buf.length;
        head = (((position + positionOffset) % len) + len) % len;
        headPosition = position;
    }
    
    private void ensureSpace(int len) {
        int size = size();
        while (buf.length - size <= len) {
            growBuffer();
        }
    }
    
}
