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

package edu.upennlib.testsaxparser.clean;

import edu.upennlib.ingestor.sax.xsl.MyBoundedQueue;
import java.util.Iterator;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author michael
 */
public class MyBoundedQueueTest {

    public MyBoundedQueueTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    MyBoundedQueue<String> instance;

    @Before
    public void setUp() {
        instance = new MyBoundedQueue<String>(4);
        instance.add("one");
        instance.add("two");
        instance.add("three");
    }

    @Test
    public void testAdd() {
    }

    @Test
    public void testOffer() {
    }

    @Test
    public void testRemove_0args() {
    }

    @Test
    public void testIsFull() {
    }

    @Test
    public void testPoll() {
    }

    @Test
    public void testElement() {
    }

    @Test
    public void testPeek() {
    }

    @Test
    public void testSize() {
    }

    @Test
    public void testIsEmpty() {
    }

    @Test
    public void testContains() {
    }

    @Test
    public void testIterator() {
        Iterator<String> iter = instance.iterator();
        System.out.println();
        System.out.println("forward iterator:");
        while (iter.hasNext()) {
            System.out.println(iter.next());
        }
    }

    @Test
    public void testReverseIterator() {
        Iterator<String> iter = instance.reverseIterator();
        System.out.println();
        System.out.println("reverse iterator:");
        while (iter.hasNext()) {
            System.out.println(iter.next());
        }
    }

    @Test
    public void testToArray_0args() {
    }

    @Test
    public void testToArray_ObjectArr() {
    }

    @Test
    public void testRemove_Object() {
    }

    @Test
    public void testContainsAll() {
    }

    @Test
    public void testAddAll() {
    }

    @Test
    public void testRemoveAll() {
    }

    @Test
    public void testRetainAll() {
    }

    @Test
    public void testClear() {
    }

}