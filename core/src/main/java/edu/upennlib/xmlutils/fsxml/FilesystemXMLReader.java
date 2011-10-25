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

package edu.upennlib.xmlutils.fsxml;

import edu.upennlib.xmlutils.Element;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;
import java.util.logging.Level;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
import org.apache.log4j.Logger;
import org.xml.sax.ContentHandler;
import org.xml.sax.DTDHandler;
import org.xml.sax.EntityResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.AttributesImpl;

public abstract class FilesystemXMLReader implements XMLReader {

    public static final boolean DEFAULT_TOLERATE_UNREADABLE_DIRECTORIES = true;
    private boolean tolerateUnreadableDirectories = DEFAULT_TOLERATE_UNREADABLE_DIRECTORIES;
    private static final String NS_PREFIXES_FEATURE_KEY = "http://xml.org/sax/features/namespace-prefixes";
    public static final String URI = "http://upennlib.edu/fsxml";
    public static final String PREFIX = "fsxml";
    private static final Logger logger = Logger.getLogger(FilesystemXMLReader.class);
    private static final Map<String,Boolean> unmodifiableFeatures;
    private static final Map<String,Boolean> modifiableFeatures = new HashMap<String, Boolean>();
    private static final Map<String, Object> ignorableProperties;
    private final Element root;
    private final Map<String, String> additionalNamespaceDeclarations;
    private final Stack<String> additionalNsPrefixStack;
    private static final URL xIncludeDirContentsXsl;
    private static Templates xIncludeDirContentsTemplates;

    static {
        xIncludeDirContentsXsl = FilesystemXMLReader.class.getClassLoader().getResource("incl_dir_contents.xsl");
        modifiableFeatures.put(NS_PREFIXES_FEATURE_KEY, false);
        Map<String, Boolean> tmpFeatures = new HashMap<String, Boolean>();
        tmpFeatures.put("http://xml.org/sax/features/namespaces", true);
        tmpFeatures.put("http://xml.org/sax/features/validation", false);
        unmodifiableFeatures = Collections.unmodifiableMap(tmpFeatures);
        ignorableProperties = new HashMap<String, Object>();
        ignorableProperties.put("http://xml.org/sax/properties/lexical-handler", null); // we know we won't be generating any lexical events.
    }

    public static InputStream getXIncludeDirContentsXsl() throws IOException {
        return xIncludeDirContentsXsl.openStream();
    }

    protected enum FsxmlElement {
        file(URI, PREFIX, "file"),
        dir(URI, PREFIX, "dir"),
        symlink(URI, PREFIX, "symlink"),
        unreadable(URI, PREFIX, "unreadable");
        public final String uri;
        public final String localName;
        public final String qName;
        private FsxmlElement(String uri, String prefix, String localName) {
            this.uri = uri;
            this.localName = localName;
            if (prefix != null && !"".equals(prefix)) {
                qName = prefix + ":" + localName;
            } else {
                qName = localName;
            }
        }
    }
    private enum FsxmlAttribute {
        symlink("", "", "symlink"),
        symlinkTarget("", "", "symlinkTarget"),
        length("", "", "length"),
        lastModified("", "", "lastModified"),
        absolutePath("", "", "absolutePath"),
        name("", "", "name");
        public final String uri;
        public final String localName;
        public final String qName;
        private FsxmlAttribute(String uri, String prefix, String localName) {
            this.uri = uri;
            this.localName = localName;
            if (prefix != null && !"".equals(prefix)) {
                qName = prefix + ":" + localName;
            } else {
                qName = localName;
            }
        }
    }
    private ContentHandler ch;
    private ErrorHandler eh;
    private EntityResolver er;
    private DTDHandler dh;

    public static void main(String[] args) throws FileNotFoundException, TransformerConfigurationException, TransformerException, IOException {
        FilesystemXMLReader instance = new FilesystemXMLReaderImpl();
        instance.setFollowSymlinks(false);
        File root = new File("/repo");
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream("/tmp/listingImpl.xml"));
        TransformerFactory tf = TransformerFactory.newInstance();
        Transformer t = tf.newTransformer();
        t.setOutputProperty(OutputKeys.INDENT, "yes");
        t.transform(new SAXSource(instance, new InputSource(root.getAbsolutePath())), new StreamResult(bos));
        bos.close();
    }

    public FilesystemXMLReader(Element root, Map<String, String> namespaceDeclarations) {
        this.root = root;
        if (namespaceDeclarations != null) {
            additionalNamespaceDeclarations = Collections.unmodifiableMap(namespaceDeclarations);
            additionalNsPrefixStack = new Stack<String>();
        } else {
            additionalNamespaceDeclarations = null;
            additionalNsPrefixStack = null;
        }
    }

    public FilesystemXMLReader newInstance() {
        return new FilesystemXMLReaderImpl();
    }

    public boolean isTolerateUnreadableDirectories() {
        return tolerateUnreadableDirectories;
    }

    public void setTolerateUnreadableDirectories(boolean tolerate) {
        tolerateUnreadableDirectories = tolerate;
    }

    private boolean followSymlinks = false;

    public boolean isFollowSymlinks() {
        return followSymlinks;
    }

    public void setFollowSymlinks(boolean follow) {
        followSymlinks = follow;
    }

    @Override
    public boolean getFeature(String name) throws SAXNotRecognizedException, SAXNotSupportedException {
        if (unmodifiableFeatures.containsKey(name)) {
            return unmodifiableFeatures.get(name);
        } else if (modifiableFeatures.containsKey(name)) {
            return modifiableFeatures.get(name);
        } else {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }

    @Override
    public void setFeature(String name, boolean value) throws SAXNotRecognizedException, SAXNotSupportedException {
        if (unmodifiableFeatures.containsKey(name)) {
            if (value != unmodifiableFeatures.get(name)) {
                throw new UnsupportedOperationException("does not support setting " + name + " to " + value);
            }
        } else if (modifiableFeatures.containsKey(name)) {
            modifiableFeatures.put(name, value);
        } else {
            throw new UnsupportedOperationException("Not supported yet." + name + value);
        }
    }

    @Override
    public Object getProperty(String name) throws SAXNotRecognizedException, SAXNotSupportedException {
        if (ignorableProperties.containsKey(name)) {
            return ignorableProperties.get(name);
        } else {
            throw new UnsupportedOperationException("Not supported yet." + name);
        }
    }

    @Override
    public void setProperty(String name, Object value) throws SAXNotRecognizedException, SAXNotSupportedException {
        if (ignorableProperties.containsKey(name)) {
            ignorableProperties.put(name, value);
        } else {
            throw new UnsupportedOperationException("Not supported yet." + name);
        }
    }

    @Override
    public void setEntityResolver(EntityResolver resolver) {
        er = resolver;
    }

    @Override
    public EntityResolver getEntityResolver() {
        return er;
    }

    @Override
    public void setDTDHandler(DTDHandler handler) {
        dh = handler;
    }

    @Override
    public DTDHandler getDTDHandler() {
        return dh;
    }

    @Override
    public void setContentHandler(ContentHandler handler) {
        ch = handler;
    }

    @Override
    public ContentHandler getContentHandler() {
        return ch;
    }

    @Override
    public void setErrorHandler(ErrorHandler handler) {
        eh = handler;
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return eh;
    }

    @Override
    public void parse(InputSource input) throws IOException, SAXException {
        logger.trace("parsing "+input.getSystemId());
        parse(input.getSystemId());
    }

    public static final String XMLNS_URI = "http://www.w3.org/2000/xmlns/";
    public static final String XMLNS_PREFIX = "xmlns";

    @Override
    public void parse(String systemId) throws IOException, SAXException {
        File rootFile = new File(systemId);
        ch.startDocument();
        ch.startPrefixMapping(PREFIX, URI);
        if (modifiableFeatures.get(NS_PREFIXES_FEATURE_KEY)) {
            atts.addAttribute(XMLNS_URI, PREFIX, XMLNS_PREFIX + ":" + PREFIX, "CDATA", URI);
            if (additionalNamespaceDeclarations != null) {
                for (Entry<String, String> e : additionalNamespaceDeclarations.entrySet()) {
                    ch.startPrefixMapping(e.getKey(), e.getValue());
                    atts.addAttribute(XMLNS_URI, e.getKey(), XMLNS_PREFIX + ":" + e.getKey(), "CDATA", e.getValue());
                    additionalNsPrefixStack.push(e.getKey());
                }
            }
        }
        if (root != null) {
            ch.startElement(root.uri, root.localName, root.qName, atts);
            atts.clear();
        }
        process(rootFile);
        if (root != null) {
            ch.endElement(root.uri, root.localName, root.qName);
        }
        if (additionalNamespaceDeclarations != null) {
            String prefix;
            while ((prefix = additionalNsPrefixStack.pop()) != null) {
                ch.endPrefixMapping(prefix);
            }
        }
        ch.endPrefixMapping(PREFIX);
        ch.endDocument();
    }

    private final AttributesImpl atts = new AttributesImpl();

    private void startFile(FsxmlElement type, File f, File canonFile, boolean isSymlink) throws SAXException, IOException {
        atts.addAttribute(FsxmlAttribute.name.uri, FsxmlAttribute.name.localName, FsxmlAttribute.name.qName, "CDATA", f.getName());
        atts.addAttribute(FsxmlAttribute.absolutePath.uri, FsxmlAttribute.absolutePath.localName, FsxmlAttribute.absolutePath.qName, "CDATA", f.getAbsolutePath());
        atts.addAttribute(FsxmlAttribute.length.uri, FsxmlAttribute.length.localName, FsxmlAttribute.length.qName, "CDATA", Long.toString(f.length()));
        atts.addAttribute(FsxmlAttribute.lastModified.uri, FsxmlAttribute.lastModified.localName, FsxmlAttribute.lastModified.qName, "CDATA", Long.toString(f.lastModified()));
        if (isSymlink) {
            symlinks.push(canonFile.getAbsolutePath());
            atts.addAttribute(FsxmlAttribute.symlink.uri, FsxmlAttribute.symlink.localName, FsxmlAttribute.symlink.qName, "CDATA", "true");
            atts.addAttribute(FsxmlAttribute.symlinkTarget.uri, FsxmlAttribute.symlinkTarget.localName, FsxmlAttribute.symlinkTarget.qName, "CDATA", canonFile.getCanonicalPath());
        } else {
            atts.addAttribute(FsxmlAttribute.symlink.uri, FsxmlAttribute.symlink.localName, FsxmlAttribute.symlink.qName, "CDATA", "false");
        }
        ch.startElement(URI, type.localName, type.qName, atts);
    }

    private void endFile(FsxmlElement type, File canonFile, boolean isSymlink) throws SAXException, IOException {
        if (isSymlink) {
            if (!symlinks.pop().equals(canonFile.getAbsolutePath())) {
                throw new RuntimeException();
            }
        }
        ch.endElement(URI, type.localName, type.qName);
    }

    private boolean writeOutput = false;

    protected abstract boolean startWriteOutput(FsxmlElement type, File f, File[] children);

    private void process(File f) throws SAXException, IOException {
        if (!f.exists()) {
            throw new IOException("file does not exist: "+f);
        }
        FsxmlElement type;
        File[] children = null;
        File canonFile = getCanonFile(f);
        boolean isSymlink = isSymlink(canonFile);
        if (isSymlink && (!isFollowSymlinks() || isCircularSymlink(canonFile, symlinks, isSymlink))) {
            type = FsxmlElement.symlink;
        } else if (!f.isDirectory()) {
            type = FsxmlElement.file;
        } else {
            type = FsxmlElement.dir;
            children = f.listFiles();
            if (children != null) {
                Arrays.sort(children);
            }
        }
        boolean preWriteOutput = writeOutput;
        if (!writeOutput) {
            writeOutput = startWriteOutput(type, f, children);
        }
        if (writeOutput) {
            startFile(type, f, canonFile, isSymlink);
        }
        if (type == FsxmlElement.dir) {
            if (children != null) {
                for (File child : children) {
                    atts.clear();
                    process(child);
                }
            } else if (tolerateUnreadableDirectories) {
                atts.clear();
                ch.startElement(FsxmlElement.unreadable.uri, FsxmlElement.unreadable.localName, FsxmlElement.unreadable.qName, atts);
                ch.endElement(FsxmlElement.unreadable.uri, FsxmlElement.unreadable.localName, FsxmlElement.unreadable.qName);
            } else {
                throw new RuntimeException("unreadable directory: "+f);
            }
        }
        if (writeOutput) {
            endFile(type, canonFile, isSymlink);
            if (!preWriteOutput) {
                writeOutput = false;
            }
        }
    }

    private static File getCanonFile(File file) throws IOException {
        if (file == null) {
            throw new NullPointerException("File must not be null");
        }
        File canon;
        if (file.getParent() == null) {
            canon = file;
        } else {
            File canonDir = file.getParentFile().getCanonicalFile();
            canon = new File(canonDir, file.getName());
        }
        return canon;
    }

    private static boolean isSymlink(File canonFile) throws IOException {
        return !canonFile.getCanonicalFile().equals(canonFile.getAbsoluteFile());
    }

    private static boolean isCircularSymlink(File canonFile, Stack<String> symlinkPathStack, boolean isSymlink) throws IOException {
        if (!isSymlink) {
            return false;
        }
        String target = canonFile.getCanonicalPath();
        if (canonFile.getAbsolutePath().startsWith(target)) {
            return true;
        }
        for (String path : symlinkPathStack) {
            if (path.startsWith(target)) {
                return true;
            }
        }
        return false;
    }
    private final Stack<String> symlinks = new Stack<String>();

    private static class FilesystemXMLReaderImpl extends FilesystemXMLReader {

        private FilesystemXMLReaderImpl() {
            super(null, null);
        }

        @Override
        protected boolean startWriteOutput(FsxmlElement type, File f, File[] children) {
            return true;
        }

    }
}
