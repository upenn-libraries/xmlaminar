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

import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Michael Gibney
 */
public class Node<T extends DelegatingSubdividable<ProcessingState, T, Node<T>>> implements ParentSubdividable<ProcessingState, Node<T>, T> {

    private static final Logger LOG = LoggerFactory.getLogger(Node.class);
    private volatile Node next;
    private volatile Node previous;
    private final T value;
    private final Queue<Node<T>> homePool;
    private final ProcessingQueue<T> processingQueue;
    private final Lock previousLock = new ReentrantLock();
    private final Condition previousChanged = previousLock.newCondition();

    Node(T value, Queue<Node<T>> homePool, ProcessingQueue<T> pq) {
        this.value = value;
        this.homePool = homePool;
        this.processingQueue = pq;
    }

    Node(Node<T> previous, T value, Queue<Node<T>> homePool, ProcessingQueue<T> pq) {
        this(value, homePool, pq);
        this.previous = previous;
        previous.next = this;
    }

    public static void main(String[] args) throws Exception {
        TXMLFilter.main(args);
    }
    
    @Override
    public Node<T> subdivide(ExecutorService executor) {
        Node<T> newNode = processingQueue.getSubdivideNode(value);
        insert(newNode);
        return newNode;
    }

    Node<T> getNext(ProcessingState requireState, Node<T> tail) {
        Node<T> nex = null;
        boolean unlocked = true;
        try {
            do {
                do {
                    (nex = next).previousLock.lock();
                    unlocked = false;
                } while (nex != next && (unlocked = unlock(nex)));
                if (nex.getState() != requireState) {
                    do {
                        if (processingQueue.isFinished() && nex == tail) {
                            return null;
                        }
                        nex.previousChanged.await();
                    } while (nex == next && nex.getState() != requireState);
                }
            } while ((nex != next && (unlocked = unlock(nex))) 
                    || (!nex.removePreLockAcquired(this, false) && (unlocked = unlock(nex))));
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        } finally {
            if (!unlocked) {
                unlock(nex);
            }
        }
        return nex;
    }

    public ProcessingState getState() {
        return state;
    }

    void insert(Node<T> node) {
        previousLock.lock();
        try {
            Node<T> prev = previous;
            node.next = this;
            node.previous = prev;
            previous = node;
            prev.next = node;
            previousChanged.signalAll();
        } finally {
            previousLock.unlock();
        }
    }


    void reset() {
        next = null;
        previous = null;
        value.reset();
        this.state = ProcessingState.READY;
        if (homePool != null) {
            homePool.add(this);
        }
    }
    
    String getPoolType() {
        return homePool == null ? null : processingQueue.getPoolType(homePool);
    }

    private void setWorkComplete() {
        previousLock.lock();
        try {
            previousChanged.signalAll();
        } finally {
            previousLock.unlock();
        }
    }

    private void failed() {
        if (!canSubdivide()) {
            value.drop();
            remove(true);
        } else {
            value.subdivide(processingQueue.getWorkExecutor());
        }
    }

    private static boolean unlock(Node node) {
        node.previousLock.unlock();
        return true;
    }
    
    private void remove(boolean reset) {
        boolean unlocked = true;
        try {
            do {
                previousLock.lock();
                unlocked = false;
            } while (!removePreLockAcquired(previous, reset) && (unlocked = unlock(this)));
        } finally {
            if (!unlocked) {
                unlock(this);
            }
        }
    }
    
    boolean removePreLockAcquired(Node<T> prev, boolean reset) {
        Node<T> nex = null;
        boolean unlocked = true;
        try {
            do {
                if (unlocked = !(nex = next).previousLock.tryLock()) {
                    return false;
                }
            } while (nex != next && (unlocked = unlock(nex)));
            next.previous = previous;
            previous.next = next;
            previous = null;
            next = null;
            previousChanged.signalAll();
            nex.previousChanged.signalAll();
        } finally {
            if (!unlocked) {
                unlock(nex);
            }
        }
        if (reset) {
            reset();
        }
        return true;
    }

    private volatile ProcessingState state = ProcessingState.READY;
    
    @Override
    public void setState(ProcessingState state) {
        this.state = state;
        switch (state) {
            case HAS_INPUT:
                processingQueue.addToWorkQueue(value);
                break;
            case HAS_SUBDIVIDED_INPUT:
                processingQueue.addToHeadOfWorkQueue(value);
                break;
            case HAS_OUTPUT:
                setWorkComplete();
                break;
            case FAILED:
                failed();
                break;
            case READY:
                //remove();
                break;
        }
    }

    @Override
    public T getChild() {
        return value;
    }

    @Override
    public boolean canSubdivide() {
        return value.canSubdivide();
    }

    boolean isPrevious(Node<T> node) {
        previousLock.lock();
        try {
            if (node == previous) {
                previousChanged.signalAll();
                return true;
            } else {
                return false;
            }
        } finally {
            previousLock.unlock();
        }
    }

}
