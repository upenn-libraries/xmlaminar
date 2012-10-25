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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 * @author Michael Gibney
 */
public class Node<T extends DelegatingSubdividable<ProcessingState, T, Node<T>>> implements ParentSubdividable<ProcessingState, Node<T>, T> {

    private volatile Node next;
    private volatile Node previous;
    private final T value;
    private final Queue<Node<T>> homePool;
    private final ProcessingQueue<T> processingQueue;
    private volatile boolean workComplete = false;
    private final AtomicBoolean pre = new AtomicBoolean(false);
    private final AtomicBoolean post = new AtomicBoolean(false);

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

    boolean isWorkComplete() {
        return workComplete;
    }

    @Override
    public Node<T> subdivide() {
        Node<T> newNode = processingQueue.getSubdivideNode(value);
        insert(newNode);
        return newNode;
    }

    Node<T> getNext() {
        return next;
    }

    void insert(Node<T> node) {
        AtomicBoolean preTmp;
        do {
            while (!(preTmp = previous.post).compareAndSet(false, true)) {
            }
        } while (!pre.compareAndSet(false, true) && strictSetFalse(preTmp));
        node.next = this;
        node.previous = previous;
        previous.next = node;
        previous = node;
        strictSetFalse(preTmp);
        strictSetFalse(pre);
    }

    private static boolean strictSetFalse(AtomicBoolean b) {
        if (!b.compareAndSet(true, false)) {
            throw new IllegalStateException();
        }
        return true;
    }

    private void reset() {
        next = null;
        previous = null;
        workComplete = false;
        value.reset();
        homePool.add(this);
    }

    private void setWorkComplete() {
        workComplete = true;
        processingQueue.workComplete(this);
    }

    private void failed() {
        if (!canSubdivide()) {
            remove();
        } else {
            value.subdivide();
        }
    }

    void remove() {
        AtomicBoolean preTmp;
        AtomicBoolean postTmp;
        do {
            do {
                do {
                    while (!(preTmp = previous.post).compareAndSet(false, true)) {
                    }
                } while (!(postTmp = next.pre).compareAndSet(false, true) && strictSetFalse(preTmp));
            } while (!pre.compareAndSet(false, true) && strictSetFalse(preTmp) && strictSetFalse(postTmp));
        } while (!post.compareAndSet(false, true) && strictSetFalse(preTmp) && strictSetFalse(postTmp) && strictSetFalse(pre));
        next.previous = previous;
        previous.next = next;
        strictSetFalse(preTmp);
        strictSetFalse(pre);
        strictSetFalse(postTmp);
        strictSetFalse(post);
        reset();
    }

    @Override
    public void setState(ProcessingState state) {
        switch (state) {
            case HAS_INPUT:
                processingQueue.addToWorkQueue(value);
            case HAS_SUBDIVIDED_INPUT:
                processingQueue.addToHeadOfWorkQueue(value);
            case HAS_OUTPUT:
                setWorkComplete();
            case FAILED:
                failed();
            case READY:
                remove();
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

}
