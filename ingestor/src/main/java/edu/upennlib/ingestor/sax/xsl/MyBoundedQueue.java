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

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.upennlib.ingestor.sax.xsl;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;

/**
 *
 * @author michael
 */
    public class MyBoundedQueue<T> implements Queue {

        private final T[] queue;
        private final int sizeLimit;
        private int size = 0;
        private int head = 0;
        private int tail = 0;

        public MyBoundedQueue(int size) {
            queue = (T[]) new Object[size];
            sizeLimit = size;
        }

        @Override
        public boolean add(Object e) {
            if (size < sizeLimit) {
                queue[tail] = (T) e;
                size++;
                tail = (tail + 1) % sizeLimit;
                return true;
            } else {
                return false;
            }
        }

        @Override
        public boolean offer(Object e) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object remove() {
            if (size < 1) {
                throw new NoSuchElementException();
            } else {
                size--;
                Object toReturn = queue[head];
                head = (head + 1) % sizeLimit;
                return toReturn;
            }
        }

        public boolean isFull() {
            return size == sizeLimit;
        }

        @Override
        public Object poll() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object element() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object peek() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int size() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isEmpty() {
            return size < 1;
        }

        @Override
        public boolean contains(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator iterator() {
            return new MyBoundedIterator();
        }

        private class MyBoundedIterator implements Iterator {

            int remaining = size;
            int cursor = head;

            @Override
            public boolean hasNext() {
                return remaining > 0;
            }

            @Override
            public Object next() {
                if (hasNext()) {
                    remaining--;
                    Object toReturn = queue[cursor];
                    cursor = (cursor + 1) % sizeLimit;
                    return toReturn;
                } else {
                    throw new NoSuchElementException();
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Iterator "+this+" is unmodifiable");
            }
        }

        public Iterator reverseIterator() {
            return new MyReverseIterator();
        }

        private class MyReverseIterator implements Iterator {

            int remaining = size;
            int cursor = tail - 1 % sizeLimit;

            @Override
            public boolean hasNext() {
                return remaining > 0;
            }

            @Override
            public Object next() {
                if (hasNext()) {
                    remaining--;
                    Object toReturn = queue[cursor];
                    cursor = (cursor - 1) % sizeLimit;
                    return toReturn;
                } else {
                    throw new NoSuchElementException();
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Not supported");
            }
        }

        @Override
        public Object[] toArray() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object[] toArray(Object[] a) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean containsAll(Collection c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll(Collection c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeAll(Collection c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean retainAll(Collection c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            size = 0;
            head = 0;
            tail = 0;
        }
    }
