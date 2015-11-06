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

package edu.upennlib.paralleltransformer;

import com.sun.org.apache.xerces.internal.impl.XMLEntityManager;
import edu.upennlib.xmlutils.Resettable;
import edu.upennlib.xmlutils.VolatileXMLFilterImpl;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

/**
 *
 * @author magibney
 */
public class InputSourceXMLReader extends VolatileXMLFilterImpl {

    private final InputSource source;
    private Reader ownReader;
    private final Resettable resetter;
    private boolean parsed = false;
    
    public InputSourceXMLReader(InputSource source) {
        this(null, source, null);
    }

    public InputSourceXMLReader(XMLReader parent, InputSource source) {
        this(parent, source, null);
    }
    
    public InputSourceXMLReader(XMLReader parent, InputSource source, Resettable resetter) {
        super(parent);
        this.source = source;
        this.resetter = resetter;
    }
    
    private static Reader openNewReader(InputSource input) {
        if (input.getCharacterStream() == null && input.getByteStream() == null) {
            try {
                return new InputStreamReader(new File(input.getSystemId()).toURI().toURL().openStream(), StandardCharsets.UTF_8);
            } catch (MalformedURLException ex) {
                throw new RuntimeException(ex);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        } else {
            return null;
        }
    }
    
    @Override
    public void parse(String systemId) throws SAXException, IOException {
        throw new UnsupportedOperationException("not supported");
    }

    @Override
    public void parse(InputSource input) throws SAXException, IOException {
        InputSource in;
        if (!parsed) {
            parsed = true;
            in = source;
        } else {
            if (resetter != null) {
                resetter.reset();
                System.err.println("XXX just reset this");
            }
            in = new InputSource(source.getSystemId());
        }
        if ((ownReader = openNewReader(in)) != null) {
            in.setCharacterStream(ownReader);
        }
        try {
            super.parse(in);
        } catch (SAXException e) {
            closeReaders();
            throw e;
        } catch (IOException e) {
            closeReaders();
            throw e;
        } catch (RuntimeException e) {
            closeReaders();
            throw e;
        } catch (Error e) {
            closeReaders();
            throw e;
        }
    }
    
    private void closeReaders() throws IOException {
        if (ownReader != null) {
            ownReader.close();
        }
    }

}
