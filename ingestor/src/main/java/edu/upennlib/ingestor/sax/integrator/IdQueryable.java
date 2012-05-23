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

package edu.upennlib.ingestor.sax.integrator;

import edu.upennlib.configurationutils.IndexedPropertyConfigurable;
import java.io.EOFException;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

/**
 *
 * @author michael
 */
public interface IdQueryable extends Runnable, IndexedPropertyConfigurable {

    public Comparable getId() throws EOFException;

    public boolean self();

    public int getLevel() throws EOFException;

    public void skipOutput();

    public void writeOutput(ContentHandler ch);

//    public void writeOuterStartElement(ContentHandler ch, boolean asSelf);
//
//    public void writeInnerStartElement(ContentHandler ch);
//
//    public void writeInnerEndElement(ContentHandler ch);
//
//    public void writeOuterEndElement(ContentHandler ch);
//
    public void writeEndElements(ContentHandler ch, int lowerLevel, boolean aggregating) throws SAXException;

    public void writeStartElements(ContentHandler ch, int lowerLevel, boolean aggregating) throws SAXException;

    public void writeRootElement(ContentHandler ch) throws SAXException;

    public void step();

    public boolean isFinished();

}
