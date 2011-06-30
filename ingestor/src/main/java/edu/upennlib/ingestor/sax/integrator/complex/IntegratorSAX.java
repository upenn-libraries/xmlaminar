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

package edu.upennlib.ingestor.sax.integrator.complex;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

/**
 *
 * @author michael
 */
public class IntegratorSAX implements Runnable {
    private StatefulXMLFilter[] sources;
    private final IntegratorOutputNode rootOutputNode = new IntegratorOutputNode(null);

    private void blockUntilAtRest() {
        for (StatefulXMLFilter sxf : sources) {
            sxf.blockUntilAtRest();
        }
    }

    private Comparable evaluateNextLeastId() {
        int leastIdSourceLevel = -1;
        Comparable leastId = null;
        for (int i = 0; i < sources.length; i++) {
            int level = sources[i].getLevel();
            if (level > leastIdSourceLevel) {
                leastIdSourceLevel = level;
                leastId = sources[i].getId();
            } else if (level == leastIdSourceLevel && sources[i].getId().compareTo(leastId) < 0) {
                leastId = sources[i].getId();
            }
        }
        return leastId;
    }

    private void wakeUpdatedSources() {
        for (StatefulXMLFilter sxf : sources) {
            if (sxf.isUpdated()) {
                synchronized(sxf) {
                    sxf.notify();
                }
            }
        }
    }

    ContentHandler ch;

    @Override
    public void run() {
        while (true) {
            blockUntilAtRest();
            if (rootOutputNode.readyToWrite()) {
                try {
                    rootOutputNode.writeOutput(ch);
                } catch (SAXException ex) {
                    throw new RuntimeException(ex);
                }
            }
            Comparable leastId = evaluateNextLeastId();
            rootOutputNode.prepare(leastId);
            wakeUpdatedSources();
        }
    }
}
