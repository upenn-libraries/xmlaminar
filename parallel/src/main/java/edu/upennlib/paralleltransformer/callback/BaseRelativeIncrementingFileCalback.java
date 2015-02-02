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
import javax.xml.transform.sax.SAXSource;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLFilter;

/**
 *
 * @author magibney
 */
public class BaseRelativeIncrementingFileCalback extends BaseRelativeFileCallback {

    private final int suffixLength;
    
    public BaseRelativeIncrementingFileCalback(File inputBase, File outputBase, Transformer t, String outputExtension, boolean replaceExtension, int suffixLength, XMLFilter outputFilter, boolean gzipOutput) {
        super(inputBase, outputBase, t, outputExtension, replaceExtension, gzipOutput);
        this.suffixLength = suffixLength;
        this.ifc = new IncrementingFileCallback(0, t, suffixLength, outputFilter, gzipOutput);
    }

    public BaseRelativeIncrementingFileCalback(File inputBase, File outputBase, Transformer t, String outputExtension, int suffixLength, XMLFilter outputFilter, boolean gzipOutput) {
        super(inputBase, outputBase, t, outputExtension, gzipOutput);
        this.suffixLength = suffixLength;
        this.ifc = new IncrementingFileCallback(0, t, suffixLength, outputFilter, gzipOutput);
    }

    public BaseRelativeIncrementingFileCalback(File inputBase, File outputBase, Transformer t, int suffixLength, XMLFilter outputFilter, boolean gzipOutput) {
        super(inputBase, outputBase, t, gzipOutput);
        this.suffixLength = suffixLength;
        this.ifc = new IncrementingFileCallback(0, t, suffixLength, outputFilter, gzipOutput);
    }

    private final IncrementingFileCallback ifc;
    private String lastPath;
    
    @Override
    public void callback(VolatileSAXSource source) throws SAXException, IOException {
        InputSource input = source.getInputSource();
        String systemId = input.getSystemId();
        File nextFile = convertInToOutBase(systemId);
        String path = nextFile.getAbsolutePath();
        if (!path.equals(lastPath)) {
            String ext;
            if (replaceExtension) {
                ext = outputExtension;
            } else {
                ext = StreamCallback.getExtension(systemId);
            }
            ifc.setBaseFile(nextFile, ext);
            lastPath = path;
        }
        ifc.callback(source);
    }
    
    
    
}
