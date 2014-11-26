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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

/**
 *
 * @author magibney
 */
public class BaseRelativeIncrementingFileCalback extends BaseRelativeFileCallback {

    private static final TransformerFactory tf = TransformerFactory.newInstance("net.sf.saxon.TransformerFactoryImpl", null);
    
    private final int suffixLength;
    
    public BaseRelativeIncrementingFileCalback(File inputBase, File outputBase, Transformer t, String outputExtension, boolean replaceExtension, int suffixLength) {
        super(inputBase, outputBase, t, outputExtension, replaceExtension);
        this.suffixLength = suffixLength;
    }

    public BaseRelativeIncrementingFileCalback(File inputBase, File outputBase, Transformer t, String outputExtension, int suffixLength) {
        super(inputBase, outputBase, t, outputExtension);
        this.suffixLength = suffixLength;
    }

    public BaseRelativeIncrementingFileCalback(File inputBase, File outputBase, Transformer t, int suffixLength) {
        super(inputBase, outputBase, t);
        this.suffixLength = suffixLength;
    }

    @Override
    public void callback(XMLReader reader, String systemId) throws SAXException, IOException {
        throw new UnsupportedOperationException("not implemented");
    }

    private final Map<String, IncrementingFileCallback> callbacks = new HashMap<String, IncrementingFileCallback>();
    
    @Override
    public void callback(XMLReader reader, InputSource input) throws SAXException, IOException {
        File nextFile = convertInToOutBase(input.getSystemId());
        String path = nextFile.getAbsolutePath();
        IncrementingFileCallback ifc = callbacks.get(path);
        if (ifc == null) {
            String ext;
            if (replaceExtension) {
                ext = outputExtension;
            } else {
                ext = StreamCallback.getExtension(input.getSystemId());
            }
            try {
                ifc = new IncrementingFileCallback(0, tf.newTransformer(), suffixLength, nextFile, ext);
            } catch (TransformerConfigurationException ex) {
                throw new RuntimeException(ex);
            }
            callbacks.put(path, ifc);
        }
        ifc.callback(reader, input);
    }
    
    
    
}
