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

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.xml.sax.Attributes;

class StructuralStartEvent {

    public static enum StructuralStartEventType { DOCUMENT, PREFIX_MAPPING, ELEMENT }

    public static Thread worker;

    public static void main(String[] args) throws InterruptedException {
        ExecutorService ex = Executors.newCachedThreadPool();
        Future<?> future = ex.submit(new Runnable() {

            @Override
            public void run() {
                worker = Thread.currentThread();
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException ex1) {
                    try {
                        System.out.println("interrupted once");
                        Thread.sleep(10000);
                        System.out.println("I finished what I was doing!");
                    } catch (InterruptedException ex2) {
                        throw new RuntimeException(ex2);
                    }
                }
            }
        });
        Thread.sleep(250);
        worker.interrupt();
        System.out.println(future.isCancelled()+", "+future.isDone());
        try {
            future.get();
        } catch (ExecutionException ex1) {
            System.out.println("execution exception");
        } catch (CancellationException ex1) {
            System.out.println("cancellation exception");
        }
        System.out.println("main is exiting");
        ex.shutdown();
    }

    public final StructuralStartEventType type;
    public final String one;
    public final String two;
    public final String three;
    public final Attributes atts;

    public StructuralStartEvent() {
        type = StructuralStartEventType.DOCUMENT;
        one = null;
        two = null;
        three = null;
        atts = null;
    }

    public StructuralStartEvent(String prefix, String uri) {
        type = StructuralStartEventType.PREFIX_MAPPING;
        this.one = prefix;
        this.two = uri;
        three = null;
        atts = null;
    }

    public StructuralStartEvent(String uri, String localName, String qName, Attributes atts) {
        type = StructuralStartEventType.ELEMENT;
        one = uri;
        two = localName;
        three = qName;
        this.atts = atts;
    }

    @Override
    public String toString() {
        switch (type) {
            case DOCUMENT:
                return StructuralStartEventType.DOCUMENT.toString();
            case PREFIX_MAPPING:
                return StructuralStartEventType.PREFIX_MAPPING.toString().concat(one);
            case ELEMENT:
                return StructuralStartEventType.ELEMENT.toString().concat(three);
            default:
                throw new IllegalStateException();
        }
    }
}