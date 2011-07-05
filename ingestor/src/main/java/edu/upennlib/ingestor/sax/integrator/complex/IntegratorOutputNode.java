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

package edu.upennlib.ingestor.sax.integrator.complex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.TTCCLayout;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

/**
 *
 * @author michael
 */
public class IntegratorOutputNode implements Runnable, IdQueryable {

    private final StatefulXMLFilter inputFilter;
    private StatefulXMLFilter outputFilter;
    private LinkedHashMap<String, IntegratorOutputNode> children = new LinkedHashMap<String, IntegratorOutputNode>();
    private String[] childElementNames;
    private IdQueryable[] childNodes;
    private boolean[] requireForWrite;

    public static enum State {POTENTIALLY_WRITABLE, WRITABLE, NOT_WRITABLE}
    private State state = State.POTENTIALLY_WRITABLE;

    protected Logger logger = Logger.getLogger(getClass());

    @Override
    public void run() {
        if (children.isEmpty()) {
            if (inputFilter == null) {
                throw new IllegalStateException();
            } else {
                outputFilter = inputFilter;
                outputFilter.run();
            }
        } else {
            outputFilter = new StatefulXMLFilter();
            if (inputFilter != null) {
                names.addFirst(null);
                nodes.addFirst(inputFilter);
                requires.addFirst(true);
            }
            for (Entry<String, IntegratorOutputNode> e : children.entrySet()) {
                Thread t = new Thread(e.getValue(), e.getKey());
                t.start();
            }
            while (true) {
                // get least highest level, least id
                
            }
        }
    }

    public IntegratorOutputNode(StatefulXMLFilter payload) {
        Appender appender = new ConsoleAppender(new TTCCLayout(), "System.out");
        logger.addAppender(appender);
        logger.setLevel(Level.TRACE);
        if (payload != null) {
            this.inputFilter = payload;
        } else {
            this.inputFilter = null;
        }
    }

    boolean xxxRequireForWrite;

    @Override
    public void skipOutput() {
        outputFilter.skipOutput();
    }

    @Override
    public void writeOutput(ContentHandler ch) {
        outputFilter.writeOutput(ch);
    }

    @Override
    public void writeStartElements(ContentHandler ch) {
        outputFilter.writeStartElements(ch);
    }

    @Override
    public void writeEndElements(ContentHandler ch) {
        outputFilter.writeEndElements(ch);
    }
    
    private AttributesImpl attRunner = new AttributesImpl();

    @Override
    public boolean self() {
        return outputFilter.self();
    }

    @Override
    public Comparable getId() {
        return outputFilter.getId();
    }

    @Override
    public int getLevel() {
        return outputFilter.getLevel();
    }

    public void addChild(String childElementName, IntegratorOutputNode child, boolean requireForWrite) {
        if (childElementName == null || child == null) {
            throw new NullPointerException();
        }
        names.add(childElementName);
        nodes.add(child);
        requires.add(requireForWrite);
    }

    LinkedList<String> names = new LinkedList<String>();
    LinkedList<IdQueryable> nodes = new LinkedList<IdQueryable>();
    LinkedList<Boolean> requires = new LinkedList<Boolean>();
}
