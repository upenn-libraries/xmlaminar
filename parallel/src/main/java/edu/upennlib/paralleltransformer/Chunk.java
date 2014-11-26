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

import edu.upennlib.paralleltransformer.callback.XMLReaderCallback;
import edu.upennlib.xmlutils.UnboundedContentHandlerBuffer;
import java.io.IOException;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.sax.SAXSource;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

/**
 *
 * @author Michael Gibney
 */
public class Chunk extends DelegatingSubdividable<ProcessingState, Chunk, Node<Chunk>> {

    private int recordCount = -1;
    private InputSource inSource;
    private UnboundedContentHandlerBuffer in = new UnboundedContentHandlerBuffer();
    private UnboundedContentHandlerBuffer out = new UnboundedContentHandlerBuffer();
    private final Transformer transformer;
    private final Templates templates;


    @Override
    public boolean canSubdivide() {
        return recordCount > 1;
    }

    public Chunk(Templates t) {
        templates = t;
        try {
            this.transformer = t.newTransformer();
        } catch (TransformerConfigurationException ex) {
            throw new RuntimeException(ex);
        }
    }

    private final LevelSplittingXMLFilter splitter = new LevelSplittingXMLFilter();
    private final InputSource dummy = new InputSource();

    @Override
    public Chunk subdivide() {
        Chunk newChunk = super.subdivide();
        try {
            populateSubdividedParts(newChunk);
        } catch (SAXException ex) {
            throw new RuntimeException(ex);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return newChunk;
    }

    private void populateSubdividedParts(final Chunk newChunk) throws SAXException, IOException {
        final int newChunkSize = (recordCount + 1) / 2;
        splitter.setChunkSize(newChunkSize);
        swapIO();
        splitter.setParent(out);
        splitter.setOutputCallback(new XMLReaderCallback() {

            boolean initialized = false;
            
            @Override
            public void callback(XMLReader reader, InputSource input) throws SAXException, IOException {
                if (!initialized) {
                    initialized = true;
                    reader.setContentHandler(newChunk.in);
                    reader.parse(input);
                    newChunk.setRecordCount(newChunkSize);
                    newChunk.setState(ProcessingState.HAS_SUBDIVIDED_INPUT);
                } else {
                    reader.setContentHandler(in);
                    reader.parse(input); // reads second half into this.in.
                    splitter.setOutputCallback(null); // free this callback for GC
                    out.clear();
                    recordCount = recordCount - newChunkSize;
                    setState(ProcessingState.HAS_SUBDIVIDED_INPUT);
                }
            }

            @Override
            public void callback(XMLReader reader, String systemId) throws SAXException, IOException {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public void finished() {
                // NOOP
            }
        });
    }

    private void swapIO() {
        UnboundedContentHandlerBuffer tmp = in;
        in = out;
        out = tmp;
        in.clear();
    }

    public void setRecordCount(int count) {
        recordCount = count;
    }

    @Override
    protected void reset() {
        in.clear();
        out.clear();
        recordCount = -1;
    }

    @Override
    public void run() {
        transformer.reset();
        try {
            transformer.transform(new SAXSource(in, dummy), new SAXResult(out));
            setState(ProcessingState.HAS_OUTPUT);
        } catch (TransformerException ex) {
            setState(ProcessingState.FAILED);
        }
    }

    @Override
    public Chunk newInstance() {
        return new Chunk(templates);
    }

    public void writeOutputTo(ContentHandler ch) throws SAXException {
        out.flush(ch, null);
    }

    public UnboundedContentHandlerBuffer getInput(InputSource inSource) {
        this.inSource = inSource;
        return in;
    }

    BufferSAXSource getOutput() {
        return new BufferSAXSource(out, inSource);
    }
    
    static class BufferSAXSource extends SAXSource {
        
        private final UnboundedContentHandlerBuffer reader;
        
        private BufferSAXSource(UnboundedContentHandlerBuffer reader, InputSource in) {
            super(reader, in);
            this.reader = reader;
        }

        @Override
        public UnboundedContentHandlerBuffer getXMLReader() {
            return reader;
        }

        @Override
        public void setXMLReader(XMLReader reader) {
            throw new UnsupportedOperationException("property XMLReader is read-only on "+BufferSAXSource.class);
        }
        
    }

}
