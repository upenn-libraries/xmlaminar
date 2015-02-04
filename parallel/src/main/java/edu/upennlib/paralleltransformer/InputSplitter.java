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

import edu.upennlib.xmlutils.VolatileSAXSource;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Iterator;
import java.util.Scanner;
import java.util.regex.Pattern;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

/**
 *
 * @author magibney
 */
public class InputSplitter implements QueueSourceXMLFilter.IteratorWrapper<VolatileSAXSource> {

    private static final String DEFAULT_DELIM = System.lineSeparator();
    private final int chunkSize;
    private final Pattern inputDelimPattern;
    private final String outputDelim;
    
    public InputSplitter(int chunkSize, String inputDelimPattern, int inputDelimPatternFlags, String outputDelim) {
        this.chunkSize = chunkSize;
        this.outputDelim = outputDelim;
        this.inputDelimPattern = Pattern.compile(inputDelimPattern, inputDelimPatternFlags);
    }
    
    public InputSplitter(int chunkSize, String delim) {
        this(chunkSize, delim, Pattern.LITERAL, delim);
    }
    
    public InputSplitter(int chunkSize) {
        this(chunkSize, DEFAULT_DELIM);
    }
    
    private static Scanner initializeScanner(InputSource is, Pattern inputDelimPattern) {
        Scanner s;
        Reader r;
        InputStream stream;
        String systemId;
        if ((r = is.getCharacterStream()) != null) {
            s = new Scanner(r);
        } else if ((stream = is.getByteStream()) != null) {
            s = new Scanner(stream);
        } else if ((systemId = is.getSystemId()) != null) {
            try {
                s = new Scanner(new File(systemId));
            } catch (FileNotFoundException ex) {
                throw new RuntimeException(ex);
            }
        } else {
            throw new IllegalStateException("input source contains no information");
        }
        s.useDelimiter(inputDelimPattern);
        return s;
    }
    
    private static class SplittingReader extends Reader {

        private int count = 0;
        private int index = -1;
        private String current = null;
        private boolean delim = false;
        private Scanner s;
        private final int chunkSize;
        private final String outputDelim;
        
        private SplittingReader(int chunkSize, String outputDelim) {
            this.chunkSize = chunkSize;
            this.outputDelim = outputDelim;
        }
        
        @Override
        public void reset() {
            count = 0;
            index = -1;
            current = null;
            delim = false;
        }
        
        public void init(Scanner s) {
            reset();
            this.s = s;
        }
        
        public boolean isFinished() {
            return (s == null || !s.hasNext()) && (index < 0 || index >= current.length());
        }
        
        @Override
        public int read(char[] cbuf, final int off, final int len) throws IOException {
            int outIndex = off;
            final int maxDestEnd = off + len;
            while (outIndex < maxDestEnd) {
                if (index > -1) {
                    int maxSrcEnd = index + maxDestEnd - outIndex;
                    if (current.length() < maxSrcEnd) {
                        current.getChars(index, current.length(), cbuf, outIndex);
                        outIndex += (current.length() - index);
                        index = -1;
                    } else {
                        current.getChars(index, maxSrcEnd, cbuf, outIndex);
                        index = maxSrcEnd;
                        return len;
                    }
                } else if (s.hasNext() && (delim || count++ < chunkSize)) {
                    if (delim || count == 1) {
                        current = s.next();
                        index = 0;
                        delim = false;
                    } else {
                        current = outputDelim;
                        index = 0;
                        delim = true;
                    }
                } else {
                    int ret = outIndex - off;
                    return ret == 0 ? -1 : ret;
                }
            }
            throw new AssertionError("should never get here");
        }

        @Override
        public void close() throws IOException {
            if (!s.hasNext()) {
                s.close();
                s = null;
            }
        }
        
    }

    @Override
    public Iterator<VolatileSAXSource> wrapIterator(Iterator<VolatileSAXSource> base) {
        return new SplittingIterator(base, chunkSize, outputDelim, inputDelimPattern);
    }
    
    private static class SplittingIterator implements Iterator<VolatileSAXSource> {

        private final Iterator<VolatileSAXSource> base;
        private final Pattern inputDelimPattern;
        private final SplittingReader sr;
        private String currentSystemId;
        private XMLReader currentXMLReader;
        
        private SplittingIterator(Iterator<VolatileSAXSource> base, int chunkSize, String outputDelim, Pattern inputDelimPattern) {
            this.base = base;
            this.inputDelimPattern = inputDelimPattern;
            this.sr = new SplittingReader(chunkSize, outputDelim);
        }
        
        @Override
        public boolean hasNext() {
            if (!sr.isFinished()) {
                return true;
            } else {
                return base.hasNext();
            }
        }

        @Override
        public VolatileSAXSource next() {
            if (sr.isFinished()) {
                VolatileSAXSource backing = base.next();
                InputSource backingIn = backing.getInputSource();
                currentXMLReader = backing.getXMLReader();
                currentSystemId = backingIn.getSystemId();
                sr.init(initializeScanner(backingIn, inputDelimPattern));
            }
            sr.reset();
            InputSource splitIn = new InputSource(sr);
            splitIn.setSystemId(currentSystemId);
            return new VolatileSAXSource(currentXMLReader, splitIn);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Not supported.");
        }
        
    }
    
}
