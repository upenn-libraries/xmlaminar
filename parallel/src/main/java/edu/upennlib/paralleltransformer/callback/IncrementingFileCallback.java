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
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

/**
 *
 * @author magibney
 */
public class IncrementingFileCallback implements XMLReaderCallback {
    private static final int DEFAULT_START_INDEX = 0;
    private static final int DEFAULT_SUFFIX_SIZE = 5;

    private final File parentFile;
    private final String namePrefix;
    private final String postSuffix;
    private final String suffixFormat;
    private final Transformer t;
    private int i;

    private static String getDefaultSuffixFormat(int suffixLength) {
        return "-%0" + suffixLength + 'd';
    }

    public IncrementingFileCallback(String prefix) throws TransformerConfigurationException {
        this(TransformerFactory.newInstance().newTransformer(), prefix);
    }

    public IncrementingFileCallback(Transformer t, String prefix) {
        this(DEFAULT_START_INDEX, t, prefix);
    }

    public IncrementingFileCallback(int start, Transformer t, String prefix) {
        this(start, t, prefix, "");
    }

    public IncrementingFileCallback(int start, Transformer t, String prefix, String postSuffix) {
        this(start, t, DEFAULT_SUFFIX_SIZE, prefix, postSuffix);
    }

    public IncrementingFileCallback(int start, Transformer t, int suffixSize, String prefix, String postSuffix) {
        this(start, t, getDefaultSuffixFormat(suffixSize), prefix, postSuffix);
    }

    public IncrementingFileCallback(int start, Transformer t, String suffixFormat, String prefix, String postSuffix) {
        this(start, t, suffixFormat, new File(prefix), postSuffix);
    }

    public IncrementingFileCallback(int start, Transformer t, int suffixLength, File prefix, String postSuffix) {
        this(start, t, getDefaultSuffixFormat(suffixLength), prefix, postSuffix);
    }

    public IncrementingFileCallback(int start, Transformer t, String suffixFormat, File prefix, String postSuffix) {
        this.i = start;
        this.t = t;
        this.parentFile = prefix.getParentFile();
        this.namePrefix = prefix.getName();
        this.postSuffix = postSuffix;
        this.suffixFormat = suffixFormat;
    }

    @Override
    public void callback(SAXSource source) throws SAXException, IOException {
        File nextFile = new File(parentFile, namePrefix + String.format(suffixFormat, i++) 
                + (postSuffix != null ? postSuffix : StreamCallback.getExtension(source.getInputSource().getSystemId())));
        StreamCallback.writeToFile(source, nextFile, t);
    }

    @Override
    public void finished(Throwable t) {
    }
    
}
