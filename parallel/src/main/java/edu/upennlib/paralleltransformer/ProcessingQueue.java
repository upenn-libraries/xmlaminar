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

import java.util.Collections;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * @author Michael Gibney
 */
public class ProcessingQueue<T extends DelegatingSubdividable<ProcessingState, T, Node<T>>> {

    private final ArrayBlockingQueue<Node<T>> mainPool;
    private final ArrayBlockingQueue<Node<T>> subdividePool;

    private ExecutorService workExecutor;
    private Set<Future<?>> activeWorkTasks;
    private ExecutorCompletionService workCompletionService;
    
    private final Node<T> head = new Node<T>(null, null, this);
    private final Node<T> tail = new Node<T>(head, null, null, this);

    private final Lock headLock = new ReentrantLock();
    private final Condition headWait = headLock.newCondition();
    
    private volatile boolean finished = false;

    public ProcessingQueue(int size, T templateInstance) {
        System.out.println(head+", "+tail);
        mainPool = new ArrayBlockingQueue<Node<T>>(size);
        for (int i = 0; i < size; i++) {
            mainPool.add(newNode(templateInstance, mainPool));
        }
        subdividePool = new ArrayBlockingQueue<Node<T>>(size);
    }

    private Set<Future<?>> initializeCompletionService() {
        Set<Future<?>> previousActiveTasks = activeWorkTasks;
        activeWorkTasks = Collections.synchronizedSet(new HashSet<Future<?>>());
        workCompletionService = new ExecutorCompletionService(workExecutor, new TaskRemovalQueue(activeWorkTasks));
        return previousActiveTasks;
    }

    private static class TaskRemovalQueue<T> extends BlockingQueueImpl<T> {

        private final Set<T> removalTarget;

        private TaskRemovalQueue(Set<T> removalTarget) {
            this.removalTarget = removalTarget;
        }

        @Override
        public boolean add(T e) {
            if (removalTarget.remove(e)) {
                return true;
            } else {
                throw new AssertionError();
            }
        }

    }

    public ExecutorService getWorkExecutor() {
        return workExecutor;
    }

    public void setWorkExecutor(ExecutorService workExecutor) {
        this.workExecutor = workExecutor;
    }

    /**
     * Accessed by one thread only.
     * @return
     * @throws InterruptedException
     */
    public T nextIn() throws InterruptedException {
        Node<T> next = mainPool.take();
        tail.insert(next);
        return next.getChild();
    }

    /**
     * Accessed by one thread only -- only acquire lock when waiting for work to
     * complete.
     * @return
     * @throws InterruptedException
     */
    public T nextOut() throws InterruptedException {
        Node<T> next = head.getNext();
        if (!next.isWorkComplete()) {
            try {
                headLock.lock();
                while (!(next = head.getNext()).isWorkComplete()) {
                    headWait.await();
                }
            } finally {
                headLock.unlock();
            }
        }
        next.remove();
        return next.getChild();
    }

    private Node<T> newNode(T templateInstance, Queue<Node<T>> pool) {
        T instance = templateInstance.newInstance();
        Node<T> node = new Node<T>(instance, pool, this);
        instance.setParent(node);
        return node;
    }

    Node<T> getSubdivideNode(T templateInstance) {
        Node<T> node = subdividePool.poll();
        if (node == null) {
            node = newNode(templateInstance, subdividePool);
        }
        return node;
    }

    void workComplete(Node<T> node) {
        if (node == head.getNext()) {
            try {
                headLock.lock();
                headWait.signal();
            } finally {
                headLock.unlock();
            }
        }
    }

    void addToWorkQueue(T value) {
        //workQueue.addLast(value);
        activeWorkTasks.add(workCompletionService.submit(value, null));
    }

    void addToHeadOfWorkQueue(T value) {
        //workQueue.addFirst(value);
        activeWorkTasks.add(workCompletionService.submit(value, null));
    }

    void reset() {
        Set<Future<?>> toCancel = initializeCompletionService();
        for (Future<?> future : toCancel) {
            future.cancel(true);
        }
        finished = false;
    }

    public void finished() {
        finished = true;
    }

    public boolean isFinished() {
        return finished || !isEmpty();
    }

    private boolean isEmpty() {
        return tail.getNext() == head;
    }

}