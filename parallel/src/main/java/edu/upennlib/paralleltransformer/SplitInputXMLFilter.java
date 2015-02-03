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

import edu.upennlib.paralleltransformer.callback.OutputCallback;
import edu.upennlib.paralleltransformer.callback.XMLReaderCallback;
import edu.upennlib.xmlutils.VolatileSAXSource;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Scanner;
import java.util.regex.Pattern;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 *
 * @author magibney
 */
public class SplitInputXMLFilter extends QueueSourceXMLFilter implements OutputCallback {

    private XMLReaderCallback callback;
    
    private final int chunkSize;
    private final Pattern inputDelimPattern;
    private final String outputDelim;
    private Scanner s;
    
    public SplitInputXMLFilter(int chunkSize, String inputDelimPattern, int inputDelimPatternFlags, String outputDelim) {
        this.chunkSize = chunkSize;
        this.outputDelim = outputDelim;
        this.inputDelimPattern = Pattern.compile(inputDelimPattern, inputDelimPatternFlags);
    }
    
    public SplitInputXMLFilter(int chunkSize, String delim) {
        this(chunkSize, delim, Pattern.LITERAL, delim);
    }
    
    public SplitInputXMLFilter(int chunkSize) {
        this(chunkSize, System.lineSeparator());
    }
    
    @Override
    protected void initialParse(VolatileSAXSource in) {
        repeatParse(in);
    }

    @Override
    protected void repeatParse(VolatileSAXSource in) {
        InputSource is = in.getInputSource();
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
        }
        s.useDelimiter(inputDelimPattern);
        while (s.hasNext()) {
            InputSource downstream = new InputSource(in.getSystemId());
            try {
                splittingReader.reset();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            downstream.setCharacterStream(splittingReader);
            try {
                callback.callback(new VolatileSAXSource(downstream));
            } catch (SAXException ex) {
                throw new RuntimeException(ex);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }
    
    private final SplittingReader splittingReader = new SplittingReader();
    
    private class SplittingReader extends Reader {

        private int count = 0;
        private int index = -1;
        private String current = null;
        private boolean delim = false;
        
        @Override
        public void reset() throws IOException {
            count = 0;
            index = -1;
            current = null;
            delim = false;
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
            }
        }
        
    }

    @Override
    protected void finished() throws SAXException {
        callback.finished(null);
    }

    @Override
    public boolean allowOutputCallback() {
        return true;
    }

    @Override
    public XMLReaderCallback getOutputCallback() {
        return callback;
    }

    @Override
    public void setOutputCallback(XMLReaderCallback callback) {
        this.callback = callback;
    }
    
}
