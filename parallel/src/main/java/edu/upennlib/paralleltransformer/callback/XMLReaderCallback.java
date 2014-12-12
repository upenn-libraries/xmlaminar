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

package edu.upennlib.paralleltransformer.callback;

import java.io.IOException;
import javax.xml.transform.sax.SAXSource;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

/**
 * Provides a callback interface for handling calls to XMLReader <code>parse()</code>
 * methods. Implementations of the <code>callback()</code> methods need not be 
 * synchronous -- i.e., they may return before calling the <code>parse()</code> 
 * method on the provided XMLReader. If such synchronization is required, it should 
 * be implemented in the calling class. 
 * 
 * @author magibney
 */
public interface XMLReaderCallback {

    void callback(SAXSource source) throws SAXException, IOException;

    void finished(Throwable t);
    
}
