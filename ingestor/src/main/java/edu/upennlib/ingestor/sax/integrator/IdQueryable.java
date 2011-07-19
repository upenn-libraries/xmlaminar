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

import edu.upennlib.ingestor.sax.utils.StartElementExtension;
import java.io.EOFException;
import org.xml.sax.ContentHandler;

/**
 *
 * @author michael
 */
public interface IdQueryable extends Runnable, StartElementExtension {
    public Comparable getId() throws EOFException;
    public boolean self();
    public int getLevel() throws EOFException;
    public void skipOutput();
    public void writeOutput(ContentHandler ch);
    public void writeOuterStartElement(ContentHandler ch, boolean asSelf, boolean startElementExtension);
    public void writeInnerStartElement(ContentHandler ch, boolean startElementExtension);
    public void writeInnerEndElement(ContentHandler ch, boolean startElementExtension);
    public void writeOuterEndElement(ContentHandler ch, boolean startElementExtension);
    public void step();
    public boolean isFinished();
    public void setName(String name);
}
