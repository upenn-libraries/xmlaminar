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
import java.io.FilterReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.nio.CharBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Pattern;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author magibney
 */
public class InputSplitter implements QueueSourceXMLFilter.IteratorWrapper<VolatileSAXSource> {

    private static final String DEFAULT_DELIM = System.lineSeparator();
    private final int chunkSize;
    private final Pattern inputDelimPattern;
    private final String outputDelim;
    private final int lookaheadFactor;
    
    public InputSplitter(int chunkSize, int lookaheadFactor, String inputDelimPattern, int inputDelimPatternFlags, String outputDelim) {
        this.chunkSize = chunkSize;
        this.lookaheadFactor = lookaheadFactor;
        this.outputDelim = outputDelim;
        this.inputDelimPattern = Pattern.compile(inputDelimPattern, inputDelimPatternFlags);
    }
    
    public InputSplitter(int chunkSize, int lookaheadFactor, String delim) {
        this(chunkSize, lookaheadFactor, delim, Pattern.LITERAL, delim);
    }
    
    public InputSplitter(int chunkSize, int lookaheadFactor) {
        this(chunkSize, lookaheadFactor, DEFAULT_DELIM);
    }
    
    public InputSplitter(int chunkSize) {
        this(chunkSize, 0);
    }
    
    public static void main(String[] args) throws Exception {
        int chunkSize = 1;
        int lookahead = 1;
        XMLReader dummy = new XMLFilterImpl();
        List<VolatileSAXSource> list = new ArrayList<VolatileSAXSource>();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            InputSource in = new InputSource("something"+i);
            sb.setLength(0);
            for (int j = 0; j < 10; j++) {
                sb.append(i).append('-').append(j).append('\n');
            }
            in.setCharacterStream(new StringReader(sb.toString()));
            list.add(new VolatileSAXSource(dummy, in));
        }
        Iterator<VolatileSAXSource> iter = list.iterator();
        //Iterator<VolatileSAXSource> iter = Collections.singletonList(new VolatileSAXSource(new InputSource("../cli/work/ids.txt"))).iterator();
        Iterator<Scanner> ss = new ScannerSupplier(iter, Pattern.compile(DEFAULT_DELIM, Pattern.LITERAL));
        SplittingReader sr = new SplittingReader(chunkSize, DEFAULT_DELIM, ss);
        Iterator<Reader> blah;
        if (lookahead < 1) {
            blah = sr;
        } else {
            blah = new BufferingSplittingReader(sr, lookahead);
        }
        char[] cbuf = new char[2048];
//        ArrayList<Reader> stuff = new ArrayList<Reader>();
//        int i = 0;
//        while (blah.hasNext() && i < 100) {
//            stuff.add(blah.next());
//            System.out.println("hey"+i++);
//        }
//        blah = stuff.iterator();
        while (blah.hasNext()) {
            Reader next = blah.next();
            int read;
            System.out.println("start");
            while ((read = next.read(cbuf, 0, cbuf.length)) != -1) {
                System.out.append(CharBuffer.wrap(cbuf, 0, read));
            }
            System.out.println("end");
        }
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
    
    private static class BufferingSplittingReader extends FilterReader implements Iterator<Reader> {

        private final CircularCharBuffer ccb;
        private final SplittingReader sr;
        private final int chunkSize;
        private int next = 0;
        private final ArrayDeque<Integer> boundaries = new ArrayDeque<Integer>();
        private final ArrayDeque<Integer> readers = new ArrayDeque<Integer>();
        
        public BufferingSplittingReader(SplittingReader in, int lookaheadFactor) {
            super(in);
            sr = in;
            this.chunkSize = lookaheadFactor + 1;
            this.ccb = new CircularCharBuffer();
        }
        
        @Override
        public int read(char[] cbuf, int off, int len) throws IOException {
            int ret = super.read(cbuf, off, len);
            int newBoundary;
            if (ret > 0) {
                ccb.write(cbuf, off, ret);
            } else if (boundaries.isEmpty() ? (newBoundary = ccb.getPosition()) != next :
                    (boundaries.peekLast() != (newBoundary = ccb.getPosition()) &&
                    newBoundary != next)) {
                boundaries.add(newBoundary);
            }
            return ret;
        }
        
        @Override
        public boolean hasNext() {
            return sr.hasNext() || !boundaries.isEmpty();
        }

        private final char[] cbuf = new char[2048];
        
        @Override
        public Reader next() {
            if (!sr.isFinished() || ccb.size() == 0) {
                this.in = sr.next();
            }
            int startPosition;
            Integer trimPosition;
            if (boundaries.size() >= chunkSize) {
                startPosition = next;
                trimPosition = boundaries.remove();
                next = trimPosition;
            } else {
                try {
                    while (read(cbuf, 0, cbuf.length) != -1) {
                        // populate ccb up to boundary
                    }
                    startPosition = next;
                    trimPosition = boundaries.remove();
                    next = trimPosition;
                    while (boundaries.size() < chunkSize && sr.hasNext()) {
                        this.in = sr.next();
                        while (read(cbuf, 0, cbuf.length) != -1) {
                            // populate ccb up to boundary
                        }
                    }
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
            if (readers.size() > chunkSize + 1) {
                ccb.trim(readers.remove());
            }
            readers.add(trimPosition);
            return new CCBReader(startPosition, trimPosition, ccb, this);
        }
        
        public void close(int position) throws IOException {
            Integer pos;
            int trimPosition;
            if ((pos = readers.peek()) != null && CircularCharBuffer.inOrder(pos, position)) {
                do {
                    readers.remove();
                    trimPosition = pos;
                } while ((pos = readers.peek()) != null && CircularCharBuffer.inOrder(pos, position));
                ccb.trim(trimPosition);
                close();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Not supported.");
        }
        
    }
    
    private static class CCBReader extends Reader {

        private final int trimPosition;
        private int position;
        private final CircularCharBuffer ccb;
        private final BufferingSplittingReader backing;

        public CCBReader(int startPosition, int trimPosition, CircularCharBuffer ccb, BufferingSplittingReader backing) {
            this.ccb = ccb;
            this.backing = backing;
            this.position = startPosition;
            this.trimPosition = trimPosition;
        }
        
        @Override
        public int read(char[] cbuf, int off, int len) throws IOException {
            int ret;
            if (position >= 0) {
                ret = ccb.read(cbuf, off, len, position);
                if (ret >= 0) {
                    position += ret;
                } else {
                    ret = backing.read(cbuf, off, len);
                    position = -1;
                }
            } else {
                ret = backing.read(cbuf, off, len);
            }
            return ret;
        }

        @Override
        public void close() throws IOException {
            backing.close(trimPosition);
        }
        
    }
    
    private static class SplittingReader extends Reader implements Iterator<Reader> {

        private int count = 0;
        private int index = -1;
        private String current = null;
        private boolean delim = false;
        private Scanner s;
        private final int chunkSize;
        private final String outputDelim;
        private final Iterator<Scanner> backing;
        
        private SplittingReader(int chunkSize, String outputDelim, Iterator<Scanner> backing) {
            this.chunkSize = chunkSize;
            this.outputDelim = outputDelim;
            this.backing = backing;
        }
        
        @Override
        public void reset() {
            count = 0;
            index = -1;
            current = null;
            delim = false;
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
                } else if (delim || (s != null && s.hasNext() && count++ <= chunkSize)) {
                    if (delim || count == 1) {
                        if (count <= chunkSize) {
                            current = s.next();
                            index = 0;
                        }
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
            if (s != null && !s.hasNext()) {
                s.close();
                s = null;
            }
        }

        @Override
        public boolean hasNext() {
            return !isFinished() || backing.hasNext();
        }

        @Override
        public Reader next() {
            reset();
            if (isFinished()) {
                this.s = backing.next();
            }
            return this;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Not supported.");
        }
        
    }

    @Override
    public Iterator<VolatileSAXSource> wrapIterator(Iterator<VolatileSAXSource> base) {
        return new SplittingIterator(base, chunkSize, outputDelim, inputDelimPattern, lookaheadFactor);
    }
    
    private static class SplittingIterator implements Iterator<VolatileSAXSource> {

        private final Iterator<Reader> sr;
        private final ScannerSupplier ss;
        
        private SplittingIterator(Iterator<VolatileSAXSource> base, int chunkSize, String outputDelim, Pattern inputDelimPattern, int lookaheadFactor) {
            this.ss = new ScannerSupplier(base, inputDelimPattern);
            SplittingReader splitter = new SplittingReader(chunkSize, outputDelim, ss);
            if (lookaheadFactor < 1) {
                this.sr = splitter;
            } else {
                this.sr = new BufferingSplittingReader(splitter, lookaheadFactor);
            }
        }
        
        @Override
        public boolean hasNext() {
            return sr.hasNext();
        }

        @Override
        public VolatileSAXSource next() {
            InputSource splitIn = new InputSource(sr.next());
            splitIn.setSystemId(ss.getSystemId());
            return new VolatileSAXSource(ss.getXMLReader(), splitIn);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Not supported.");
        }
        
    }
    
    private static class ScannerSupplier implements Iterator<Scanner> {

        private XMLReader xmlReader;
        private String systemId;
        private final Iterator<VolatileSAXSource> base;
        private final Pattern inputDelimPattern;

        public ScannerSupplier(Iterator<VolatileSAXSource> base, Pattern inputDelimPattern) {
            this.base = base;
            this.inputDelimPattern = inputDelimPattern;
        }
        
        public String getSystemId() {
            return systemId;
        }
        
        public XMLReader getXMLReader() {
            return xmlReader;
        }
        
        @Override
        public boolean hasNext() {
            return base.hasNext();
        }

        @Override
        public Scanner next() {
            VolatileSAXSource backing = base.next();
            InputSource backingIn = backing.getInputSource();
            xmlReader = backing.getXMLReader();
            systemId = backingIn.getSystemId();
            return initializeScanner(backingIn, inputDelimPattern);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Not supported.");
        }
        
    }
    
}
