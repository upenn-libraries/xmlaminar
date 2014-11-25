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
import java.net.URI;
import javax.xml.transform.Transformer;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

/**
 *
 * @author magibney
 */
public class BaseRelativeFileCallback implements XMLReaderCallback {

    private static final String DEFAULT_OUTPUT_EXTENSION = null;
    private static final boolean DEFAULT_REPLACE_EXTENSION = false;
    
    private final URI inputBase;
    private final URI outputBase;
    private final Transformer t;
    private final String outputExtension;
    private final boolean replaceExtension;
    
    private File validateBase(File file) {
        if (!file.isDirectory()) {
            throw new IllegalArgumentException("base file must be a directory: "+file.getAbsolutePath());
        }
        return file;
    }
    
    public BaseRelativeFileCallback(File inputBase, File outputBase, Transformer t, String outputExtension, boolean replaceExtension) {
        this.inputBase = validateBase(inputBase).toURI();
        this.outputBase = validateBase(outputBase).toURI();
        this.t = t;
        this.outputExtension = (outputExtension == null ? null : ".".concat(outputExtension));
        this.replaceExtension = replaceExtension;
    }
    
    public BaseRelativeFileCallback(File inputBase, File outputBase, Transformer t, String outputExtension) {
        this(inputBase, outputBase, t, outputExtension, DEFAULT_REPLACE_EXTENSION);
    }
    
    public BaseRelativeFileCallback(File inputBase, File outputBase, Transformer t) {
        this(inputBase, outputBase, t, DEFAULT_OUTPUT_EXTENSION);
    }
    
    @Override
    public void callback(XMLReader reader, InputSource input) throws SAXException, IOException {
        String path = input.getSystemId();
        if (outputExtension != null) {
            if (replaceExtension) {
                path = StreamCallback.getBasename(path).concat(outputExtension);
            } else {
                path = path.concat(outputExtension);
            }
        }
        URI uri = (new File(path)).toURI();
        File nextFile = new File(outputBase.resolve(inputBase.relativize(uri)));
        StreamCallback.writeToFile(reader, input, nextFile, t);
    }

    @Override
    public void callback(XMLReader reader, String systemId) throws SAXException, IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void finished() {
        // NOOP
    }
    
}
