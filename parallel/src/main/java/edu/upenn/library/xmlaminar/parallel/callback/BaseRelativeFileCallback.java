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

package edu.upenn.library.xmlaminar.parallel.callback;

import edu.upenn.library.xmlaminar.VolatileSAXSource;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import javax.xml.transform.Transformer;
import javax.xml.transform.sax.SAXSource;
import org.xml.sax.SAXException;

/**
 *
 * @author magibney
 */
public class BaseRelativeFileCallback implements XMLReaderCallback {

    private static final String DEFAULT_OUTPUT_EXTENSION = null;
    private static final boolean DEFAULT_REPLACE_EXTENSION = false;
    
    private final Path inputBase;
    private final Path outputBase;
    private final Transformer t;
    protected final String outputExtension;
    protected final boolean replaceExtension;
    private final boolean gzipOutput;
    
    private Path validateBase(File file) {
        if (!file.isDirectory()) {
            throw new IllegalArgumentException("base file must be a directory: "+file.getAbsolutePath());
        }
        return file.getAbsoluteFile().toPath().normalize();
    }
    
    public BaseRelativeFileCallback(File inputBase, File outputBase, Transformer t, String outputExtension, boolean replaceExtension, boolean gzipOutput) {
        this.inputBase = validateBase(inputBase);
        this.outputBase = validateBase(outputBase);
        this.t = t;
        this.outputExtension = (outputExtension == null ? null : ".".concat(outputExtension));
        this.replaceExtension = replaceExtension;
        this.gzipOutput = gzipOutput;
    }
    
    public BaseRelativeFileCallback(File inputBase, File outputBase, Transformer t, String outputExtension, boolean gzipOutput) {
        this(inputBase, outputBase, t, outputExtension, DEFAULT_REPLACE_EXTENSION, gzipOutput);
    }
    
    public BaseRelativeFileCallback(File inputBase, File outputBase, Transformer t, boolean gzipOutput) {
        this(inputBase, outputBase, t, DEFAULT_OUTPUT_EXTENSION, gzipOutput);
    }
    
    @Override
    public void callback(VolatileSAXSource source) throws SAXException, IOException {
        File nextFile = convertInToOut(source.getInputSource().getSystemId(), gzipOutput);
        StreamCallback.writeToFile(source, nextFile, t, gzipOutput);
    }
    
    protected File convertInToOut(String path, boolean gzipOutput) {
        if (outputExtension != null) {
            if (replaceExtension) {
                path = StreamCallback.getBasename(path).concat(outputExtension);
            } else {
                path = path.concat(outputExtension);
            }
        }
        if (gzipOutput) {
            path = path.concat(".gz");
        }
        Path tmp = new File(path).getAbsoluteFile().toPath().normalize();
        return outputBase.resolve(inputBase.relativize(tmp)).toFile();
    }
    
    protected File convertInToOutBase(String path) {
        path = StreamCallback.getBasename(path);
        Path tmp = new File(path).getAbsoluteFile().toPath().normalize();
        return outputBase.resolve(inputBase.relativize(tmp)).toFile();
    }

    @Override
    public void finished(Throwable t) {
        // NOOP
    }
    
}
