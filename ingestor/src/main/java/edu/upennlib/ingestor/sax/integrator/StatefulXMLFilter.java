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

package edu.upennlib.ingestor.sax.integrator;

import java.util.Stack;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author michael
 */
public class StatefulXMLFilter extends XMLFilterImpl {

    public static final String INTEGRATOR_URI = "http://integrator";
    public static enum State {WAIT, SKIP, STEP, PLAY}
    private State state = State.WAIT;
    private int level = -1;
    private int refLevel = -1;

    boolean selfId = false;
    private Stack<String> idType = new Stack<String>();
    private Stack<String> idString = new Stack<String>();
    private Stack<Long> idLong = new Stack<Long>();

    private boolean writable = false;

    public void updateState(IntegratorOutputNode ion) {
        if (state != State.WAIT) {
            throw new IllegalStateException();
        }
        if (!writable && ion.isPotentiallyWritable(getIdType(), getIdString())) {
            writable = true;
        }
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
        }
    }

    public String getIdType() {
        if (state != State.WAIT) {
            throw new IllegalStateException();
        }
        return idType.peek();
    }

    public String getIdString() {
        if (state != State.WAIT) {
            throw new IllegalStateException();
        }
        return idString.peek();
    }

    public long getIdLong() {
        if (state != State.WAIT) {
            throw new IllegalStateException();
        }
        return idLong.peek();
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
                String id = atts.getValue("id");
                if (id == null) {
                    throw new IllegalStateException("null id for " + uri + ", " + localName + ", " + qName);
                } else {
                    idType.push(localName);
                    idString.push(id);
                    idLong.push(Long.parseLong(id));
                }
                if ("true".equals(atts.getValue("self"))) {
                    selfId = true;
                }
                state = State.WAIT;
                while (state == State.WAIT) {
                    writable = false;
                    synchronized (this) {
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
                idType.pop();
                idString.pop();
                idLong.pop();
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

}
