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
import edu.upennlib.xmlutils.VolatileSAXSource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
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
import org.xml.sax.XMLFilter;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLFilterImpl;

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
            public void callback(VolatileSAXSource source) throws SAXException, IOException {
                XMLReader reader = source.getXMLReader();
                if (!initialized) {
                    initialized = true;
                    ContentHandler inputBuffer = newChunk.getInput(source);
                    reader.setContentHandler(inputBuffer);
                    reader.parse(source.getInputSource());
                    newChunk.setRecordCount(newChunkSize);
                    newChunk.setState(ProcessingState.HAS_SUBDIVIDED_INPUT);
                } else {
                    reader.setContentHandler(in);
                    reader.parse(source.getInputSource()); // reads second half into this.in.
                    in.setUnmodifiableParent(out.getUnmodifiableParent());
                    recordCount = recordCount - newChunkSize;
                }
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
    
    public static XMLReader getRootParent(XMLReader child) {
        if (child == null) {
            return null;
        } else {
            XMLReader parent;
            XMLReader init = child;
            XMLReader ret = null;
            List<String> blah = new ArrayList<String>(100);
            do {
                if (child instanceof UnboundedContentHandlerBuffer) {
                    parent = ((UnboundedContentHandlerBuffer)child).getUnmodifiableParent();
                } else if (child instanceof XMLFilter) {
                    parent = ((XMLFilter)child).getParent();
                } else {
                    parent = null;
                }
                blah.add(child.getClass().getSimpleName());
            } while ((child = parent) != null);
            //return child;
            System.err.println("XXX "+blah);
            return init;
        }
    }


    public static int getParentDepth(XMLFilter filter) {
        int ret = 0;
        XMLReader parent;
        List<String> chain = new ArrayList<String>(100);
        chain.add(filter.getClass().getSimpleName());
        while ((filter instanceof UnboundedContentHandlerBuffer ? 
                (parent = ((UnboundedContentHandlerBuffer)filter).getUnmodifiableParent()) != null : 
                (parent = filter.getParent()) != null) &&
                parent instanceof XMLFilter && ret < 100) {
            ret++;
            filter = (XMLFilter) parent;
            chain.add(filter.getClass().getSimpleName());
        }
        System.err.println("XXX"+chain);
        return ret;
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
        if (transformer instanceof Controller) {
            ((Controller)transformer).clearDocumentPool();
        }
        transformer.setErrorListener(devNullErrorListener);
        try {
            transformer.transform(new SAXSource(in, inSource), new SAXResult(out));
            out.setUnmodifiableParent(in.getUnmodifiableParent());
            setState(ProcessingState.HAS_OUTPUT);
        } catch (TransformerException ex) {
            this.ex = ex;
            setState(ProcessingState.FAILED);
        }
    }
    
    private TransformerException ex;
    
    private final RecordMonitorXMLFilter rl;
    
    private static class SingleRecordIdLogger extends RecordMonitorXMLFilter implements Cloneable {

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
        
        @Override
        public SingleRecordIdLogger newInstance() {
            return new SingleRecordIdLogger(this);
        }
        
    }
        
    private static class MultiRecordIdLogger extends RecordMonitorXMLFilter implements Cloneable {

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
        }

        @Override
        public String getRecordIdString() {
            return "ids=".concat(ids.toString());
        }
        
        @Override
        public MultiRecordIdLogger newInstance() {
            return new MultiRecordIdLogger(this);
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
                LOG.warn("{} failed transformation; {}", (inSource == null ? null : inSource.getSystemId()), ex.getMessageAndLocation());
            } else {
                LOG.warn("{} failed transformation for {}; {}", (inSource == null ? null : inSource.getSystemId()), rl.getRecordIdString(), ex.getMessageAndLocation());
            }
        } else if (rl != null) {
            rl.reset();
            try {
                in.flush(rl, null);
            } catch (SAXException ex) {
                // NOOP
            }
            LOG.warn("partial failure processing {}; dropped {}; {}", (inSource == null ? null : inSource.getSystemId()), rl.getRecordIdString(), ex.getMessageAndLocation());
        } else {
            LOG.warn("partial failure processing {}; dropped chunk; {}", (inSource == null ? null : inSource.getSystemId()), ex.getMessageAndLocation());
        }
        super.drop();
    }
    
    @Override
    public Chunk newInstance() {
        return new Chunk(templates, rl == null ? null : rl.newInstance(), subdivide);
    }

    public void writeOutputTo(ContentHandler ch) throws SAXException {
        out.flush(ch, null);
    }

    public ContentHandler getInput(SAXSource source) {
        this.inSource = source.getInputSource();
        XMLReader reader = source.getXMLReader();
        in.setUnmodifiableParent(reader);
        if (!subdivide && rl != null) {
            rl.setParent(reader);
            source.setXMLReader(rl);
        }
        return in;
    }
    
    VolatileSAXSource getOutput() {
        out.setFlushOnParse(true);
        return new VolatileSAXSource(out, inSource);
    }
    
}
