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

package edu.upennlib.paralleltransformer;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Michael Gibney
 */
public class BlockingQueueImpl<T> implements BlockingQueue<T> {

    @Override
    public boolean add(T e) {
        throw new AssertionError("Not supported");
    }

    @Override
    public boolean offer(T e) {
        throw new AssertionError("Not supported");
    }

    @Override
    public void put(T e) throws InterruptedException {
        throw new AssertionError("Not supported");
    }

    @Override
    public boolean offer(T e, long timeout, TimeUnit unit) throws InterruptedException {
        throw new AssertionError("Not supported");
    }

    @Override
    public T take() throws InterruptedException {
        throw new AssertionError("Not supported");
    }

    @Override
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        throw new AssertionError("Not supported");
    }

    @Override
    public int remainingCapacity() {
        throw new AssertionError("Not supported");
    }

    @Override
    public boolean remove(Object o) {
        throw new AssertionError("Not supported");
    }

    @Override
    public boolean contains(Object o) {
        throw new AssertionError("Not supported");
    }

    @Override
    public int drainTo(Collection<? super T> c) {
        throw new AssertionError("Not supported");
    }

    @Override
    public int drainTo(Collection<? super T> c, int maxElements) {
        throw new AssertionError("Not supported");
    }

    @Override
    public T remove() {
        throw new AssertionError("Not supported");
    }

    @Override
    public T poll() {
        throw new AssertionError("Not supported");
    }

    @Override
    public T element() {
        throw new AssertionError("Not supported");
    }

    @Override
    public T peek() {
        throw new AssertionError("Not supported");
    }

    @Override
    public int size() {
        throw new AssertionError("Not supported");
    }

    @Override
    public boolean isEmpty() {
        throw new AssertionError("Not supported");
    }

    @Override
    public Iterator<T> iterator() {
        throw new AssertionError("Not supported");
    }

    @Override
    public Object[] toArray() {
        throw new AssertionError("Not supported");
    }

    @Override
    public <T> T[] toArray(T[] a) {
        throw new AssertionError("Not supported");
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new AssertionError("Not supported");
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        throw new AssertionError("Not supported");
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new AssertionError("Not supported");
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new AssertionError("Not supported");
    }

    @Override
    public void clear() {
        throw new AssertionError("Not supported");
    }

}
