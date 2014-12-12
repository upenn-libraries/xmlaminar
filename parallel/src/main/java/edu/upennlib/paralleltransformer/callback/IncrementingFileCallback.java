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
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.upennlib.paralleltransformer.callback;

import java.io.File;
import java.io.IOException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import org.xml.sax.SAXException;

/**
 *
 * @author magibney
 */
public class IncrementingFileCallback implements XMLReaderCallback {
    private static final int DEFAULT_START_INDEX = 0;
    private static final int DEFAULT_SUFFIX_SIZE = 5;

    private File baseFile;
    private File parentFile;
    private String namePrefix;
    private final String postSuffix;
    private final String suffixFormat;
    private final Transformer t;
    private int i;

    private static String getDefaultSuffixFormat(int suffixLength) {
        return "-%0" + suffixLength + 'd';
    }

    public IncrementingFileCallback(String baseFile) throws TransformerConfigurationException {
        this(TransformerFactory.newInstance().newTransformer(), baseFile);
    }

    public IncrementingFileCallback(Transformer t, String baseFile) {
        this(DEFAULT_START_INDEX, t, baseFile);
    }

    public IncrementingFileCallback(int start, Transformer t, String baseFile) {
        this(start, t, baseFile, "");
    }

    public IncrementingFileCallback(int start, Transformer t, String baseFile, String postSuffix) {
        this(start, t, DEFAULT_SUFFIX_SIZE, baseFile, postSuffix);
    }

    public IncrementingFileCallback(int start, Transformer t, int suffixSize, String baseFile, String postSuffix) {
        this(start, t, getDefaultSuffixFormat(suffixSize), new File(baseFile), postSuffix);
    }
    
    public IncrementingFileCallback(int start, Transformer t, int suffixSize) {
        this(start, t, getDefaultSuffixFormat(suffixSize), null, null);
    }

    public IncrementingFileCallback(int start, Transformer t, int suffixLength, File baseFile, String postSuffix) {
        this(start, t, getDefaultSuffixFormat(suffixLength), baseFile, postSuffix);
    }

    public IncrementingFileCallback(int start, Transformer t, String suffixFormat, File baseFile, String postSuffix) {
        this.i = start;
        this.t = t;
        this.baseFile = baseFile;
        if (baseFile != null) {
            this.parentFile = baseFile.getParentFile();
            this.namePrefix = baseFile.getName();
        }
        this.postSuffix = postSuffix;
        this.suffixFormat = suffixFormat;
    }

    @Override
    public void callback(SAXSource source) throws SAXException, IOException {
        File nextFile = new File(parentFile, namePrefix + String.format(suffixFormat, i++) 
                + (postSuffix != null ? postSuffix : StreamCallback.getExtension(source.getInputSource().getSystemId())));
        StreamCallback.writeToFile(source, nextFile, t);
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
