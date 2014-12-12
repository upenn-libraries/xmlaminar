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
import edu.upennlib.xmlutils.DevNullErrorListener;
import edu.upennlib.xmlutils.UnboundedContentHandlerBuffer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import javax.xml.transform.ErrorListener;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.sax.SAXSource;
import net.sf.saxon.Controller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

/**
 *
 * @author Michael Gibney
 */
public class Chunk extends DelegatingSubdividable<ProcessingState, Chunk, Node<Chunk>> {

    private static final Logger LOG = LoggerFactory.getLogger(Chunk.class);
    private int recordCount = -1;
    private InputSource inSource;
    private UnboundedContentHandlerBuffer in = new UnboundedContentHandlerBuffer();
    private UnboundedContentHandlerBuffer out = new UnboundedContentHandlerBuffer();
    private final Transformer transformer;
    private final Templates templates;
    private final boolean subdivide;


    @Override
    public boolean canSubdivide() {
        return (subdivide ? recordCount > 1 : false);
    }

    public Chunk(Templates t, String xpath, boolean subdivide) {
        this(t, getRecordLogger(xpath, subdivide), subdivide);
    }
    
    private static RecordMonitorXMLFilter getRecordLogger(String xpath, boolean subdivide) {
        if (xpath == null) {
            return null;
        } else if (subdivide) {
            return new SingleRecordIdLogger(xpath);
        } else {
            return new MultiRecordIdLogger(xpath);
        }
    }
    
    private Chunk(Templates t, RecordMonitorXMLFilter rl, boolean subdivide) {
        this.rl = rl;
        templates = t;
        try {
            this.transformer = t.newTransformer();
        } catch (TransformerConfigurationException ex) {
            throw new RuntimeException(ex);
        }
        this.subdivide = subdivide;
    }

    private final LevelSplittingXMLFilter splitter = new LevelSplittingXMLFilter();

    @Override
    public Chunk subdivide(ExecutorService executor) {
        Chunk newChunk = super.subdivide(executor);
        try {
            populateSubdividedParts(newChunk, executor);
        } catch (SAXException ex) {
            throw new RuntimeException(ex);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return newChunk;
    }

    private void populateSubdividedParts(final Chunk newChunk, ExecutorService executor) throws SAXException, IOException {
        final int newChunkSize = (recordCount + 1) / 2;
        //final LevelSplittingXMLFilter splitter = new LevelSplittingXMLFilter();
        splitter.reset();
        splitter.setChunkSize(newChunkSize);
        swapIO();
        splitter.setParent(out);
        splitter.setOutputCallback(new XMLReaderCallback() {

            boolean initialized = false;
            
            @Override
            public void callback(XMLReader reader, InputSource input) throws SAXException, IOException {
                if (!initialized) {
                    initialized = true;
                    UnboundedContentHandlerBuffer inputBuffer = newChunk.getInput(input);
                    reader.setContentHandler(inputBuffer);
                    reader.parse(input);
                    inputBuffer.setUnmodifiableParent(out.getUnmodifiableParent());
                    newChunk.setRecordCount(newChunkSize);
                    newChunk.setState(ProcessingState.HAS_SUBDIVIDED_INPUT);
                } else {
                    reader.setContentHandler(in);
                    reader.parse(input); // reads second half into this.in.
                    in.setUnmodifiableParent(out.getUnmodifiableParent());
                    recordCount = recordCount - newChunkSize;
                }
            }

            @Override
            public void callback(XMLReader reader, String systemId) throws SAXException, IOException {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public void finished(Throwable t) {
                splitter.setOutputCallback(null); // free this callback for GC
            }
        });
        splitter.setExecutor(executor);
        splitter.parse(inSource);
        splitter.setOutputCallback(null); // free this callback for GC
        out.clear();
        setState(ProcessingState.HAS_SUBDIVIDED_INPUT);
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

    private static final ErrorListener devNullErrorListener = new DevNullErrorListener();
    
    @Override
    public void run() {
        transformer.reset();
        transformer.setErrorListener(devNullErrorListener);
        try {
            transformer.transform(new SAXSource(in, inSource), new SAXResult(out));
            setState(ProcessingState.HAS_OUTPUT);
        } catch (TransformerException ex) {
            this.ex = ex;
            setState(ProcessingState.FAILED);
        }
    }
    
    private TransformerException ex;
    
    private final RecordMonitorXMLFilter rl;
    
    private static class SingleRecordIdLogger extends RecordMonitorXMLFilter {

        private String id;

        public SingleRecordIdLogger(RecordMonitorXMLFilter prototype) {
            super(prototype);
        }
        
        public SingleRecordIdLogger(String xpath) {
            super(xpath);
        }

        @Override
        protected void reset() {
            id = null;
            super.reset();
        }
        
        @Override
        public void register(String id) throws SAXException {
            this.id = id;
            throw foundException;
        }

        @Override
        public String getRecordIdString() {
            return "id=".concat(id == null ? "null" : id);
        }
        
    }
        
    private static class MultiRecordIdLogger extends RecordMonitorXMLFilter {

        private final List<String> ids = new ArrayList<String>();

        public MultiRecordIdLogger(RecordMonitorXMLFilter prototype) {
            super(prototype);
        }
        
        public MultiRecordIdLogger(String xpath) {
            super(xpath);
        }

        @Override
        protected void reset() {
            ids.clear();
            super.reset();
        }
        
        @Override
        public void register(String id) throws SAXException {
            ids.add(id);
            throw foundException;
        }

        @Override
        public String getRecordIdString() {
            return "ids=".concat(ids.toString());
        }
        
    }
        
    private static final FoundXPathException foundException = new FoundXPathException();

    private static class FoundXPathException extends SAXException {

        @Override
        public synchronized Throwable fillInStackTrace() {
            return null;
        }

    }
    @Override
    protected void drop() {
        if (!subdivide) {
            if (rl == null) {
                LOG.warn("{} failed transormation; {}", inSource, ex.getMessageAndLocation());
            } else {
                LOG.warn("{} failed transormation for {}; {}", inSource, rl.getRecordIdString(), ex.getMessageAndLocation());
            }
        } else if (rl != null) {
            rl.reset();
            try {
                in.flush(rl, null);
            } catch (SAXException ex) {
                // NOOP
            }
            LOG.warn("dropping {}; {}", rl.getRecordIdString(), ex.getMessageAndLocation());
        } else {
            LOG.warn("dropping chunk; {}", ex.getMessageAndLocation());
        }
        super.drop();
    }
    
    @Override
    public Chunk newInstance() {
        return new Chunk(templates, rl == null ? null : new SingleRecordIdLogger(rl), subdivide);
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
