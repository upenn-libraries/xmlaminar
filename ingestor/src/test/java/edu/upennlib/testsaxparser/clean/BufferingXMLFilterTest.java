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

import edu.upennlib.ingestor.sax.xsl.BufferingXMLFilter;
import java.util.Arrays;
import java.util.Iterator;
import org.xml.sax.helpers.AttributesImpl;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author michael
 */
public class BufferingXMLFilterTest {

    public BufferingXMLFilterTest() {
    }

    static BufferingXMLFilter instance = new BufferingXMLFilter();

    @BeforeClass
    public static void setUpClass() throws Exception {
        instance.startDocument();
        instance.startPrefixMapping("pre", "http://test");
        instance.startElement("http://test", "one", "pre:one", new AttributesImpl());
        instance.startElement("http://test", "two", "pre:two", new AttributesImpl());
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() {
    }

    @Test
    public void testMain() throws Exception {
    }

    @Test
    public void testParse_InputSource() throws Exception {
    }

    @Test
    public void testClear() {
    }

    @Test
    public void testParse_String() throws Exception {
    }

    @Test
    public void testPlay() throws Exception {
    }

    @Test
    public void testPlayMostRecentStructurallyInsignificant() throws Exception {
    }

    @Test
    public void testIterator() {
        Iterator<Object[]> iter = instance.iterator();
        while (iter.hasNext()) {
            Object[] next = iter.next();
            System.out.println(Arrays.asList(next));
        }
    }

    @Test
    public void testReverseIterator() {
        Iterator<Object[]> iter = instance.reverseIterator();
        while (iter.hasNext()) {
            Object[] next = iter.next();
            System.out.println(Arrays.asList(next));
        }
    }

    @Test
    public void testStartDocument() throws Exception {
    }

    @Test
    public void testEndDocument() throws Exception {
    }

    @Test
    public void testStartPrefixMapping() throws Exception {
    }

    @Test
    public void testEndPrefixMapping() throws Exception {
    }

    @Test
    public void testStartElement() throws Exception {
    }

    @Test
    public void testEndElement() throws Exception {
    }

    @Test
    public void testCharacters() throws Exception {
    }

    @Test
    public void testIgnorableWhitespace() throws Exception {
    }

    @Test
    public void testProcessingInstruction() throws Exception {
    }

    @Test
    public void testSkippedEntity() throws Exception {
    }

}