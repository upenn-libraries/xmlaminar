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

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Stack;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author michael
 */
public class StatefulXMLFilter extends XMLFilterImpl implements Runnable {

    public static final String INTEGRATOR_URI = "http://integrator";
    public static enum State {WAIT, SKIP, STEP, PLAY}
    private State state = State.STEP;
    private int level = -1;
    private int refLevel = -1;

    private boolean updated = false;
    private boolean selfId = false;
    private LinkedList<Comparable> id = new LinkedList<Comparable>();

    private boolean writable = false;

    @Override
    public void run() {
        try {
            parse(new InputSource());
        } catch (SAXException ex) {
            throw new RuntimeException(ex);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public boolean self() {
        return selfId;
    }

    public void blockUntilAtRest() {
        while (state != State.WAIT) {
            synchronized(this) {
                try {
                    wait();
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }

    public boolean isUpdated() {
        if (state != State.WAIT) {
            throw new IllegalStateException();
        }
        return updated;
    }

    public int getLevel() {
        if (state != State.WAIT) {
            throw new IllegalStateException();
        }
        return level;
    }

    public void updateState(boolean writable) {
        if (state != State.WAIT) {
            throw new IllegalStateException();
        }
        this.writable = writable;
    }

    public void finishedUpdatingState() {
        if (state != State.WAIT) {
            throw new IllegalStateException();
        }
        if (writable) {
            if (selfId) {
                state = State.PLAY;
            } else {
                state = State.STEP;
            }
        } else {
            state = State.SKIP;
        }
        synchronized(this) {
            notify();
            try {
                wait();
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public Comparable getParentId() {
        if (state != State.WAIT) {
            throw new IllegalStateException();
        }
        if (id.size() < 2) {
            return null;
        } else {
            return id.get(1);
        }
    }

    public Comparable getId() {
        if (state != State.WAIT) {
            throw new IllegalStateException();
        }
        return id.peek();
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        level++;
        switch (state) {
            case WAIT:
                throw new IllegalStateException();
            case SKIP:
                break;
            case PLAY:
                super.startElement(uri, localName, qName, atts);
                return;
            case STEP:
                String idString = atts.getValue("id");
                if (idString == null) {
                    throw new IllegalStateException("null id for " + uri + ", " + localName + ", " + qName);
                } else {
                    Comparable localId = new IdUpenn(localName, idString);
                    id.push(localId);
                }
                if ("true".equals(atts.getValue("self"))) {
                    selfId = true;
                }
                state = State.WAIT;
                while (state == State.WAIT) {
                    writable = false;
                    synchronized (this) {
                        updated = true;
                        notify();
                        try {
                            wait();
                        } catch (InterruptedException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                }
                if (state == State.PLAY || state == State.SKIP) {
                    refLevel = level;
                }
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        switch (state) {
            case WAIT:
                throw new IllegalStateException();
            case STEP:
                selfId = false;
                id.pop();
                break;
            case PLAY:
                super.endElement(uri, localName, qName);
            case SKIP:
                if (level == refLevel) {
                    state = State.STEP;
                }
        }
        level--;
    }

    public void writeOutput(ContentHandler ch) {
        setContentHandler(ch);
        if (state != State.PLAY) {
            throw new IllegalStateException();
        }
        synchronized(this) {
            notify();
            try {
                wait();
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

}
