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

package edu.upennlib.teststaxparser;

import edu.upennlib.ingestor.subject.SubjectRemediationTableBuilder;
import edu.upennlib.ingestor.subject.SubjectNode;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author michael
 */
public class SubjectNodeTest {

    static SubjectNode table = null;

    public SubjectNodeTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        SubjectRemediationTableBuilder builder = new SubjectRemediationTableBuilder();
        table = builder.getSubjectRemediationTable();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() {
    }

    public enum HeadingPair {
        p1("Capital cities", "Capitals (Cities)"),
        p2("Protestantism and capitalism", "Capitalism--Religious aspects--Protestant churches"),
        p3("Prizes (Property captured at sea)--Law and legislation", "Prize law"),
        p4("Drug control--Government policy", "Drug control"),
        p5("Some Religion--Catechisms and creeds", "Some Religion--Catechisms and creeds"),
        p6("Conduct of life--Anecdotes, facetiae, satire, etc.", "Conduct of life--Anecdotes, facetiae, satire, etc."),
        p7("Nonexistent Heading--Female authors", "Nonexistent Heading--Women authors"),
        p8("United States--Boundary disputes", "United States--Boundaries"),
        p9("United States--Middle--Boundary disputes", "United States--Middle--Boundaries"),
        p10("Ridiculouas--Other--Middle--Boundary disputes", "Ridiculouas--Other--Middle--Boundaries"),
        p14("Dogs--Medical statistics", "Dogs--Statistics, Medical");

        public final String rawHeading;
        public final String resolvedHeading;
        HeadingPair(String rawHeading, String resolvedHeading) {
            this.rawHeading = rawHeading;
            this.resolvedHeading = resolvedHeading;
        }
    }

    @Test
    public void test1() {
        assertEquals(HeadingPair.p1.resolvedHeading, table.resolve(HeadingPair.p1.rawHeading));
    }
    @Test
    public void test2() {
        assertEquals(HeadingPair.p2.resolvedHeading, table.resolve(HeadingPair.p2.rawHeading));
    }
    @Test
    public void test3() {
        assertEquals(HeadingPair.p3.resolvedHeading, table.resolve(HeadingPair.p3.rawHeading));
    }
    @Test
    public void test4() {
        assertEquals(HeadingPair.p4.resolvedHeading, table.resolve(HeadingPair.p4.rawHeading));
    }
    @Test
    public void test5() {
        assertEquals(HeadingPair.p5.resolvedHeading, table.resolve(HeadingPair.p5.rawHeading));
    }
    @Test
    public void test6() {
        assertEquals(HeadingPair.p6.resolvedHeading, table.resolve(HeadingPair.p6.rawHeading));
    }
    @Test
    public void test7() {
        assertEquals(HeadingPair.p7.resolvedHeading, table.resolve(HeadingPair.p7.rawHeading));
    }
    @Test
    public void test8() {
        assertEquals(HeadingPair.p8.resolvedHeading, table.resolve(HeadingPair.p8.rawHeading));
    }
    @Test
    public void test9() {
        assertEquals(HeadingPair.p9.resolvedHeading, table.resolve(HeadingPair.p9.rawHeading));
    }
    @Test
    public void test10() {
        assertEquals(HeadingPair.p10.resolvedHeading, table.resolve(HeadingPair.p10.rawHeading));
    }
    @Test
    public void test14() throws InterruptedException {
        assertEquals(HeadingPair.p14.resolvedHeading, table.resolve(HeadingPair.p14.rawHeading));
    }
}
