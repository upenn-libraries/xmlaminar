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

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

/**
 *
 * @author michael
 */
public class IntegratorOutputNode {

    private final StatefulXMLFilter payload;
    HashMap<String, IntegratorOutputNode> children;

    public static enum State {POTENTIALLY_WRITABLE, WRITABLE, NOT_WRITABLE}

    private State state = State.POTENTIALLY_WRITABLE;

    public IntegratorOutputNode(StatefulXMLFilter payload) {
        this.payload = payload;
    }

    public void updatePayloadState() {
        if (payload != null) {
            payload.updateState(state == State.POTENTIALLY_WRITABLE || state == State.WRITABLE);
        }
    }

    public void resetState() {
        state = State.POTENTIALLY_WRITABLE;
    }

    public void prepare(Comparable id) {
        if (state == State.POTENTIALLY_WRITABLE) {
            if (getId() == null || id.compareTo(getId()) != 0) {
                state = State.NOT_WRITABLE;
            } else if (self()) {
                state = State.WRITABLE;
            }
        }
        switch (state) {
            case POTENTIALLY_WRITABLE:
                updatePayloadState();
            case WRITABLE:
                for (IntegratorOutputNode ion : children.values()) {
                    ion.prepare(id);
                }
        }
    }

    public boolean readyToWrite() {
        if (state == State.POTENTIALLY_WRITABLE) {
            return false;
        } else if (state == State.WRITABLE) {
            for (IntegratorOutputNode ion : children.values()) {
                if (!ion.readyToWrite()) {
                    return false;
                }
            }
        }
        return true;
    }
    private AttributesImpl attRunner = new AttributesImpl();

    public void writeOutput(ContentHandler ch) throws SAXException {
        if (payload != null) {
            payload.writeOutput();
        }
        for (Entry<String, IntegratorOutputNode> e : children.entrySet()) {
            ch.startElement("uri", e.getKey(), "qName", attRunner);
            e.getValue().writeOutput(ch);
            ch.endElement("uri", e.getKey(), "qName");
        }
    }

    public boolean self() {
        if (payload != null) {
            return payload.self();
        } else {
            for (IntegratorOutputNode ion : children.values()) {
                if (ion.self()) {
                    return true;
                }
            }
            return false;
        }
    }

    public Comparable getId() {
        if (payload != null) {
            return payload.getId();
        } else {
            Comparable id = null;
            for (IntegratorOutputNode ion : children.values()) {
                if (ion.state == State.POTENTIALLY_WRITABLE || ion.state == State.WRITABLE) {
                    Comparable tmpId = ion.getParentId();
                    if (tmpId != null && (id == null || tmpId.compareTo(id) < 0)) {
                        id = tmpId;
                    }
                }
            }
            return id;
        }
    }

    public Comparable getParentId() {
        if (payload != null) {
            return payload.getParentId();
        } else {
            return getId();
        }
    }

}
