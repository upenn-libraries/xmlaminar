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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Michael Gibney
 */
public class NewClass {

    public static void main(String[] args) throws InterruptedException {

        Set<Future<?>> syncSet = Collections.synchronizedSet(new HashSet<Future<?>>());
        BlockingQueue<Future<?>> finishedQueue = new MyBlockingQueue(syncSet);
        ExecutorService es = Executors.newCachedThreadPool();
        ExecutorCompletionService ecs = new ExecutorCompletionService(es, finishedQueue);
        syncSet.add(ecs.submit(new MyRunnable(), null));
        System.out.println(syncSet.size());
        Thread.sleep(1000);
        System.out.println(syncSet.size());
        es.shutdown();
    }




    private static class MyBlockingQueue<T> implements BlockingQueue<Future<T>> {

        private final Set<Future<T>> activeTasks;

        public MyBlockingQueue(Set<Future<T>> activeTasks) {
            this.activeTasks = activeTasks;
        }

        @Override
        public boolean add(Future<T> e) {
            System.out.println("removing "+e);
            return activeTasks.remove(e);
        }

        @Override
        public boolean offer(Future<T> e) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void put(Future<T> e) throws InterruptedException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean offer(Future<T> e, long timeout, TimeUnit unit) throws InterruptedException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Future<T> take() throws InterruptedException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Future<T> poll(long timeout, TimeUnit unit) throws InterruptedException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int remainingCapacity() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean contains(Object o) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int drainTo(Collection<? super Future<T>> c) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int drainTo(Collection<? super Future<T>> c, int maxElements) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Future<T> remove() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Future<T> poll() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Future<T> element() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Future<T> peek() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int size() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean isEmpty() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Iterator<Future<T>> iterator() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Object[] toArray() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public <T> T[] toArray(T[] a) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean addAll(Collection<? extends Future<T>> c) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

    }

    private static class MyRunnable implements Runnable {

        public MyRunnable() {
        }

        @Override
        public void run() {
            System.out.println("run!");
        }
    }

}
