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

import edu.upennlib.xmlutils.VolatileSAXSource;
import java.io.File;
import java.io.IOException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLFilter;

/**
 *
 * @author magibney
 */
public class IncrementingFileCallback implements XMLReaderCallback {
    private static final int DEFAULT_START_INDEX = 0;
    private static final int DEFAULT_SUFFIX_SIZE = 5;
    private static final boolean DEFAULT_GZIP_OUTPUT = false;

    private File baseFile;
    private File parentFile;
    private String namePrefix;
    private final String postSuffix;
    private final String suffixFormat;
    private final Transformer t;
    private final boolean gzipOutput;
    private int i;
    private final XMLFilter outputFilter;

    private static String getDefaultSuffixFormat(int suffixLength) {
        return "-%0" + suffixLength + 'd';
    }

    public IncrementingFileCallback(String baseFile, XMLFilter outputFilter) throws TransformerConfigurationException {
        this(TransformerFactory.newInstance().newTransformer(), baseFile, outputFilter);
    }

    public IncrementingFileCallback(Transformer t, String baseFile, XMLFilter outputFilter) {
        this(DEFAULT_START_INDEX, t, baseFile, outputFilter);
    }

    public IncrementingFileCallback(int start, Transformer t, String baseFile, XMLFilter outputFilter) {
        this(start, t, baseFile, "", outputFilter);
    }

    public IncrementingFileCallback(int start, Transformer t, String baseFile, String postSuffix, XMLFilter outputFilter) {
        this(start, t, DEFAULT_SUFFIX_SIZE, baseFile, postSuffix, outputFilter);
    }

    public IncrementingFileCallback(int start, Transformer t, int suffixSize, String baseFile, String postSuffix, XMLFilter outputFilter) {
        this(start, t, getDefaultSuffixFormat(suffixSize), new File(baseFile), postSuffix, outputFilter, DEFAULT_GZIP_OUTPUT);
    }
    
    public IncrementingFileCallback(int start, Transformer t, int suffixSize, XMLFilter outputFilter, boolean gzipOutput) {
        this(start, t, getDefaultSuffixFormat(suffixSize), null, null, outputFilter, gzipOutput);
    }

    public IncrementingFileCallback(int start, Transformer t, int suffixLength, File baseFile, String postSuffix, XMLFilter outputFilter, boolean gzipOutput) {
        this(start, t, getDefaultSuffixFormat(suffixLength), baseFile, postSuffix, outputFilter, gzipOutput);
    }

    public IncrementingFileCallback(int start, Transformer t, String suffixFormat, File baseFile, String postSuffix, XMLFilter outputFilter, boolean gzipOutput) {
        this.i = start;
        this.t = t;
        this.baseFile = baseFile;
        if (baseFile != null) {
            this.parentFile = baseFile.getParentFile();
            this.namePrefix = baseFile.getName();
        }
        this.postSuffix = postSuffix;
        this.suffixFormat = suffixFormat;
        this.outputFilter = outputFilter;
        this.gzipOutput = gzipOutput;
    }

    @Override
    public void callback(VolatileSAXSource source) throws SAXException, IOException {
        File nextFile = new File(parentFile, namePrefix + String.format(suffixFormat, i++)
                + (postSuffix != null ? postSuffix : StreamCallback.getExtension(source.getInputSource().getSystemId()))
                + (gzipOutput ? ".gz" : ""));
        if (outputFilter != null) {
            outputFilter.setParent(source.getXMLReader());
            source.setXMLReader(outputFilter);
        }
        StreamCallback.writeToFile(source, nextFile, t, gzipOutput);
    }
    
    public void setBaseFile(File file, String ext, boolean reset) {
        this.baseFile = file;
        this.parentFile = file.getParentFile();
        this.namePrefix = file.getName();
        if (reset) {
            reset();
        }
    }
    
    public void setBaseFile(File file, String ext) {
        setBaseFile(file, ext, true);
    }
    
    public File getBaseFile() {
        return baseFile;
    }
    
    public void reset() {
        i = 0;
    }

    @Override
    public void finished(Throwable t) {
    }
    
}
