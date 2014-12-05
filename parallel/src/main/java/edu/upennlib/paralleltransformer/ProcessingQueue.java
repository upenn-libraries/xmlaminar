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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Michael Gibney
 */
public class ProcessingQueue<T extends DelegatingSubdividable<ProcessingState, T, Node<T>>> {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessingQueue.class);
    
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
        mainPool = new ArrayBlockingQueue<Node<T>>(size);
        for (int i = 0; i < size; i++) {
            mainPool.add(newNode(templateInstance, mainPool));
        }
        subdividePool = new ArrayBlockingQueue<Node<T>>(size);
        subdividePoolCapacity = size;
    }
    
    private Set<Future<?>> initializeCompletionService() {
        Set<Future<?>> previousActiveTasks = activeWorkTasks;
        activeWorkTasks = Collections.synchronizedSet(new HashSet<Future<?>>());
        workCompletionService = new ExecutorCompletionService(workExecutor, new TaskRemovalQueue(activeWorkTasks, taskQueueLock, taskAdded));
        return previousActiveTasks;
    }

    private class TaskRemovalQueue<T extends Future<?>> extends BlockingQueueImpl<T> {

        private final Set<T> removalTarget;
        private final Lock lock;
        private final Condition condition;

        private TaskRemovalQueue(Set<T> removalTarget, Lock lock, Condition condition) {
            this.removalTarget = removalTarget;
            this.lock = lock;
            this.condition = condition;
        }

        @Override
        public boolean add(T e) {
            while (!removalTarget.remove(e)) {
                lock.lock();
                try {
                    condition.await();
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                } finally {
                    lock.unlock();
                }
            }
            try {
                e.get();
            } catch (InterruptedException ex) {
                throw new AssertionError("this should never happen", ex);
            } catch (ExecutionException ex) {
                /*
                recoverable exceptions should be caught by individual nodes; 
                exceptions caught here should kill the entire process.
                */
                LOG.error("exception escaped node; shutting down now", ex);
                ProcessingQueue.this.workExecutor.shutdownNow();
            }
            return true;
        }

    }

    public ExecutorService getWorkExecutor() {
        return workExecutor;
    }

    public void setWorkExecutor(ExecutorService workExecutor) {
        this.workExecutor = workExecutor;
    }

    private static final boolean POOL_MAIN = false;
    private static final boolean POOL_SUBDIVIDE = false;
    
    /**
     * Accessed by one thread only.
     * @return
     * @throws InterruptedException
     */
    public T nextIn() throws InterruptedException {
        Node<T> next = POOL_MAIN ? mainPool.take() : newNode(mainPool.peek().getChild(), null);
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
                    if (isFinished()) {
                        return null;
                    }
                    headWait.await();
                }
            } finally {
                headLock.unlock();
            }
        }
        // next.remove(); double-called when state set to READY
        return next.getChild();
    }

    private Node<T> newNode(T templateInstance, Queue<Node<T>> pool) {
        T instance = templateInstance.newInstance();
        Node<T> node = new Node<T>(instance, pool, this);
        instance.setParent(node);
        return node;
    }
    
    String getPoolType(Queue queue) {
        if (queue == subdividePool) {
            return "subdivide:"+subdividePoolSize.get();
        } else if (queue == mainPool) {
            return "main";
        } else {
            return null;
        }
    }

    private final int subdividePoolCapacity;
    private final AtomicInteger subdividePoolSize = new AtomicInteger(0);
    
    Node<T> getSubdivideNode(T templateInstance) {
        Node<T> node = POOL_SUBDIVIDE ? subdividePool.poll() : newNode(templateInstance, null);;
        if (node == null) {
            if (subdividePoolSize.incrementAndGet() > subdividePoolCapacity) {
                subdividePoolSize.set(subdividePoolCapacity);
                try {
                    while ((node = subdividePool.poll(100, TimeUnit.MILLISECONDS)) == null) {
                        LOG.info("test create new unassociated node to break deadlock");
                        if (head.getNext().isBlockingForSubdivide()) {
                            LOG.info("create new unassociated node to break deadlock");
                            return newNode(templateInstance, null);
                        }
                    }
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            } else {
                node = newNode(templateInstance, subdividePool);
            }
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

    void remove(Node<T> node) {
        if (head == node) {
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
        addToQueue(workCompletionService.submit(value, null));
    }

    void addToHeadOfWorkQueue(T value) {
        //workQueue.addFirst(value);
        addToQueue(workCompletionService.submit(value, null));
    }
    
    private final Lock taskQueueLock = new ReentrantLock(false);
    private final Condition taskAdded = taskQueueLock.newCondition();

    private void addToQueue(Future<?> e) {
        activeWorkTasks.add(e);
        taskQueueLock.lock();
        try {
            taskAdded.signal();
        } finally {
            taskQueueLock.unlock();
        }
    }

    void reset() {
        Set<Future<?>> toCancel = initializeCompletionService();
        if (toCancel != null) {
            for (Future<?> future : toCancel) {
                future.cancel(true);
            }
        }
        finished = false;
    }

    public void finished() {
        finished = true;
        headLock.lock();
        try {
            headWait.signal();
        } finally {
            headLock.unlock();
        }
    }

    public boolean isFinished() {
        return finished && isEmpty();
    }

    private boolean isEmpty() {
        return head.getNext() == tail;
    }

}
