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

package edu.upennlib.ingestor.sax.xsl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Queue;

    public class MyUnboundedQueue<T> implements Queue {

        private final LinkedList<MyBoundedQueue> queues = new LinkedList<MyBoundedQueue>();
        private MyBoundedQueue headQueue;
        private MyBoundedQueue tailQueue;
        private final int arraySize;
        private int size = 0;

        public MyUnboundedQueue(int arraySize) {
            this.arraySize = arraySize;
        }

        @Override
        public boolean add(Object e) {
            if (tailQueue == null || tailQueue.isFull()) {
                tailQueue = (MyBoundedQueue) getFreshQueue();
                queues.add(tailQueue);
            }
            size++;
            return tailQueue.add(e);
        }

        @Override
        public boolean offer(Object e) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object remove() {
            if (headQueue == null || headQueue.isEmpty()) {
                if (queues.isEmpty()) {
                    throw new NoSuchElementException();
                } else {
                    reclaimQueue(headQueue);
                    headQueue = queues.remove();
                }
            }
            size--;
            return headQueue.remove();
        }

        private void reclaimQueue(Queue queue) {
            if (queue != null) {
                queue.clear();
                queuePool.add(queue);
            }
        }
        private ArrayList<Queue> queuePool = new ArrayList<Queue>();

        private Queue getFreshQueue() {
            if (queuePool.isEmpty()) {
                return new MyBoundedQueue(arraySize);
            } else {
                return queuePool.remove(0);
            }
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
            return new MyUnboundedIterator();
        }

        private class MyUnboundedIterator implements Iterator {

            Iterator<Object[]> subIterator;
            Iterator<? extends Queue> queueIterator = queues.iterator();

            public MyUnboundedIterator() {
                if (headQueue != null) {
                    subIterator = headQueue.iterator();
                } else {
                    if (queueIterator.hasNext()) {
                        subIterator = queueIterator.next().iterator();
                    }
                }
            }

            @Override
            public boolean hasNext() {
                if (subIterator != null && subIterator.hasNext()) {
                    return true;
                } else if (queueIterator.hasNext()) {
                    subIterator = queueIterator.next().iterator();
                    return hasNext();
                } else {
                    return false;
                }
            }

            @Override
            public Object next() {
                if (subIterator.hasNext()) {
                    Object next = subIterator.next();
                    return next;
                } else if (queueIterator.hasNext()) {
                    subIterator = queueIterator.next().iterator();
                    return next();
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

            boolean atHeadQueueIterator = false;
            Iterator<Object[]> reverseSubIterator;
            ListIterator<MyBoundedQueue> queueIterator;

            public MyReverseIterator() {
                queueIterator = queues.listIterator(queues.size());
            }

            @Override
            public boolean hasNext() {
                if (reverseSubIterator == null || !reverseSubIterator.hasNext()) {
                    if (queueIterator.hasPrevious()) {
                        reverseSubIterator = queueIterator.previous().reverseIterator();
                        return hasNext();
                    } else if (!atHeadQueueIterator && headQueue != null) {
                        atHeadQueueIterator = true;
                        reverseSubIterator = headQueue.reverseIterator();
                        return hasNext();
                    } else {
                        return false;
                    }
                } else {
                    return reverseSubIterator.hasNext();
                }
            }

            @Override
            public Object next() {
                if (hasNext()) {
                    return reverseSubIterator.next();
                } else {
                    throw new NoSuchElementException();
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Not supported.");
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
            queues.clear();
            headQueue = null;
            tailQueue = null;
        }
    }
