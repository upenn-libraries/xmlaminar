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
    private final Lock nextLock = new ReentrantLock();
    private final Condition nextChanged = nextLock.newCondition();

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

    Node<T> getNext(ProcessingState requireState) {
        Node<T> nex;
        nextLock.lock();
        try {
            while ((nex = next).getState() != requireState) {
                if (processingQueue.isFinished()) {
                    return null;
                }
                nextChanged.await();
            }
            nex.previousLock.lock();
            try {
                while (!nex.removePreLockAcquired(this, false)) {
                    // Wait on something maybe?
                }
            } finally {
                nex.previousLock.unlock();
            }
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        } finally {
            nextLock.unlock();
        }
        return nex;
    }

    public ProcessingState getState() {
        return state;
    }

    void insert(Node<T> node) {
        Node<T> prev = null;
        boolean unlocked = true;
        boolean prevUnlocked = true;
        try {
            do {
                (prev = previous).nextLock.lock();
                prevUnlocked = false;
                while (prev == previous && (unlocked = !previousLock.tryLock())) {
                    prev.nextChanged.await();
                }
            } while (prev != previous && (unlocked = prevUnlocked = unlock(previousLock, unlocked, prev, prevUnlocked)));
            node.next = this;
            node.previous = prev;
            previous = node;
            prev.next = node;
            prev.nextChanged.signalAll();
            previousChanged.signalAll();
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        } finally {
            unlock(previousLock, unlocked, prev, prevUnlocked);
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
        Node<T> prev = null;
        boolean unlocked = true;
        boolean prevUnlocked = true;
        try {
            do {
                prev = previous;
                if (prev == null) {
                    return;
                }
                prev.nextLock.lock();
                prevUnlocked = false;
                while (prev == previous && (unlocked = !previousLock.tryLock())) {
                    prev.nextChanged.await();
                }
            } while (prev != previous && (unlocked = prevUnlocked = unlock(previousLock, unlocked, prev, prevUnlocked)));
            prev.nextChanged.signalAll();
            previousChanged.signalAll();
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        } finally {
            unlock(previousLock, unlocked, prev, prevUnlocked);
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

    private static boolean unlock(Lock previousLock, boolean unlocked, Node previous, boolean prevUnlocked) {
        try {
            if (!unlocked) {
                previousLock.unlock();
            }
        } finally {
            if (!prevUnlocked) {
                previous.nextLock.unlock();
            }
        }
        return true;
    }
    
    private void remove(boolean reset) {
        Node<T> prev = null;
        boolean unlocked = true;
        boolean prevUnlocked = true;
        try {
            do {
                do {
                    (prev = previous).nextLock.lock();
                    prevUnlocked = false;
                    while (prev == previous && (unlocked = !previousLock.tryLock())) {
                        prev.nextChanged.await();
                    }
                } while (prev != previous && (unlocked = prevUnlocked = unlock(previousLock, unlocked, prev, prevUnlocked)));
            } while (!removePreLockAcquired(prev, reset) && (unlocked = prevUnlocked = unlock(previousLock, unlocked, prev, prevUnlocked)));
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        } finally {
            unlock(previousLock, unlocked, prev, prevUnlocked);
        }
    }
    
    boolean removePreLockAcquired(Node<T> prev, boolean reset) {
        if (!nextLock.tryLock()) {
            return false;
        }
        try {
            Node<T> nex;
            if (!(nex = next).previousLock.tryLock()) {
                return false;
            }
            try {
                next.previous = previous;
                previous.next = next;
                previous = null;
                next = null;
                prev.nextChanged.signalAll();
                previousChanged.signalAll();
                nextChanged.signalAll();
                nex.previousChanged.signalAll();
            } finally {
                nex.previousLock.unlock();
            }
        } finally {
            nextLock.unlock();
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
    
    boolean isNext(Node<T> node) {
        return node == next;
    }

}
