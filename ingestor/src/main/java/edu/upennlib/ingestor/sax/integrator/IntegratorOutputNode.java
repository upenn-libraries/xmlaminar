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

package edu.upennlib.ingestor.sax.integrator;

import edu.upennlib.configurationutils.IndexedPropertyConfigurable;
import edu.upennlib.paralleltransformer.InputSourceXMLReader;
import edu.upennlib.xmlutils.DumpingLexicalXMLFilter;
import edu.upennlib.xmlutils.SAXProperties;
import edu.upennlib.xmlutils.UnboundedContentHandlerBuffer;
import edu.upennlib.xmlutils.VolatileXMLFilterImpl;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Level;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.stream.StreamResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.DTDHandler;
import org.xml.sax.EntityResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;
import org.xml.sax.ext.LexicalHandler;
import org.xml.sax.helpers.AttributesImpl;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author michael
 */
public class IntegratorOutputNode extends VolatileXMLFilterImpl implements IdQueryable {

    private static final boolean debugging = true;
    private final StatefulXMLFilter inputFilter;
    private ContentHandler output;
    private ContentHandler rawOutput;
    private String[] childElementNames;
    private IdQueryable[] childNodes;
    private Boolean[] requireForWrite;
    private String name;
    private File dumpFile;
    private static final Logger logger = LoggerFactory.getLogger(IntegratorOutputNode.class);

    private static final SAXParserFactory spf = SAXParserFactory.newInstance();
    
    static {
        spf.setNamespaceAware(true);
    }

    @Override
    public void reset() {
        if (childNodes != null) {
            for (IdQueryable child : childNodes) {
                child.reset();
            }
        }
        if (inputFilter != null) {
            inputFilter.reset();
        }
        if (rawOutput != null && rawOutput instanceof IdQueryable) {
            ((IdQueryable) rawOutput).reset();
        }
    }

    private List<String> descendentsSpring = null;
    public void setDescendentsSpring(List<String> descendentsSpring) {
        this.descendentsSpring = descendentsSpring;
    }
    public List<String> getDescendentsSpring() {
        return descendentsSpring;
    }

    private List<XMLReader> subIntegratorsSpring = null;
    public void setSubIntegratorsSpring(List<XMLReader> writeDuplicateIdsSpring) {
        this.subIntegratorsSpring = writeDuplicateIdsSpring;
    }
    public List<XMLReader> getSubIntegratorsSpring() {
        return subIntegratorsSpring;
    }

    public void initSpring() {
        HashMap<String, XMLReader> subIntegrators = new HashMap<String, XMLReader>();
        for (XMLReader integrator : subIntegratorsSpring) {
            if (!(integrator instanceof IndexedPropertyConfigurable)) {
                throw new RuntimeException(integrator+" not an instance of "+IndexedPropertyConfigurable.class.getCanonicalName());
            }
            IndexedPropertyConfigurable ipc = (IndexedPropertyConfigurable) integrator;
            if (ipc.getName() == null) {
                throw new RuntimeException("integrator "+integrator+" must have non-null name");
            }
            if (subIntegrators.put(ipc.getName(), integrator) != null) {
                throw new RuntimeException("integrator " + ipc.getName() + " (" + integrator + ") specified multiple times");
            }
        }
        for (String s : descendentsSpring) {
            String[] args = s.split("\\s*,\\s*");
            if (args.length != 3) {
                throw new IllegalArgumentException();
            }
            addDescendent(args[0], (XMLReader) subIntegrators.get(args[1]), Boolean.parseBoolean(args[2]));
        }
    }

    public void addDescendent(String path, XMLReader source, boolean requireForWrite) {
        String[] pe = path.substring(1).split("/");
        LinkedList<String> pathElements = new LinkedList<String>(Arrays.asList(pe));
        addDescendent(pathElements, source, requireForWrite);
    }

    private static final int DEPTH_LIMIT = 10;

    public StatefulXMLFilter addDescendent(LinkedList<String> pathElements, XMLReader source, boolean requireForWrite) {
        String subName = pathElements.removeFirst();
        IntegratorOutputNode subNode;
        int index;
        if ((index = names.indexOf(subName)) != -1) {
            if (pathElements.isEmpty()) {
                throw new RuntimeException("duplicate element path specification: "+subName);
            }
            subNode = (IntegratorOutputNode) nodes.get(index);
        } else {
            if (pathElements.isEmpty()) {
                StatefulXMLFilter sxf = null;
                if (source != null) {
                    if (source instanceof StatefulXMLFilter) {
                        sxf = (StatefulXMLFilter) source;
                    } else {
                        sxf = new StatefulXMLFilter(DEPTH_LIMIT);
                        sxf.setParent(source);
                    }
                }
                subNode = new IntegratorOutputNode(sxf);
                requires.add(requireForWrite);
                names.add(subName);
                nodes.add(subNode);
                return sxf;
            } else {
                subNode = new IntegratorOutputNode(null);
                requires.add(false); // Otherwise add node manually (explicitly).
                names.add(subName);
                nodes.add(subNode);
            }
        }
        if (executor != null) {
            subNode.setExecutor(executor);
        }
        return subNode.addDescendent(pathElements, source, requireForWrite);
    }

    public void setAggregating(boolean aggregating) {
        this.aggregating = aggregating;
    }

    public Boolean isAggregating() {
        return aggregating;
    }

    @Override
    public void setName(String name) {
        this.name = name;
        if (inputFilter != null) {
            inputFilter.setName(name);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    private static final Map<String,Boolean> unmodifiableFeatures;

    static {
        Map<String, Boolean> tmpFeatures = new HashMap<String, Boolean>();
        tmpFeatures.put("http://xml.org/sax/features/namespaces", true);
        tmpFeatures.put("http://xml.org/sax/features/namespace-prefixes", false);
        tmpFeatures.put("http://xml.org/sax/features/validation", false);
        tmpFeatures.put("http://xml.org/sax/features/string-interning", true); // XXX Make all of these reflect parent filters!!!
        unmodifiableFeatures = Collections.unmodifiableMap(tmpFeatures);
    }

    @Override
    public boolean getFeature(String name) throws SAXNotRecognizedException, SAXNotSupportedException {
        if (unmodifiableFeatures.containsKey(name)) {
            return unmodifiableFeatures.get(name);
        } else {
            throw new SAXNotRecognizedException("getFeature("+name+")");
        }
    }

    @Override
    public void setFeature(String name, boolean value) throws SAXNotRecognizedException, SAXNotSupportedException {
        if (unmodifiableFeatures.containsKey(name)) {
            if (value != unmodifiableFeatures.get(name)) {
                throw new SAXNotSupportedException("cannot set feature "+name+" to "+value);
            }
        } else {
            throw new SAXNotRecognizedException("setFeature("+name+", "+value+")");
        }
    }

    @Override
    public Object getProperty(String name) throws SAXNotRecognizedException, SAXNotSupportedException {
        if ("http://xml.org/sax/properties/lexical-handler".equals(name)) {
            return lexicalHandler;
        } else {
            throw new SAXNotRecognizedException("getProperty("+name+")");
        }
    }
    LexicalHandler lexicalHandler = null;

    @Override
    public void setProperty(String name, Object value) throws SAXNotRecognizedException, SAXNotSupportedException {
        if ("http://xml.org/sax/properties/lexical-handler".equals(name)) {
            lexicalHandler = (LexicalHandler) value;
        } else if (SAXProperties.EXECUTOR_SERVICE_PROPERTY_NAME.equals(name)) {
            setExecutor((ExecutorService) value);
        } else {
            throw new SAXNotRecognizedException("setFeature(" + name + ", " + value + ")");
        }
    }

    @Override
    public void setEntityResolver(EntityResolver resolver) {
        if (logger.isTraceEnabled()) {
            logger.trace("ignoring setEntityResolver("+resolver+")");
        }
    }

    @Override
    public EntityResolver getEntityResolver() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private DTDHandler dtdHandler = null;

    @Override
    public void setDTDHandler(DTDHandler handler) {
        this.dtdHandler = handler;
    }

    @Override
    public DTDHandler getDTDHandler() {
        return dtdHandler;
    }

    @Override
    public ContentHandler getContentHandler() {
        return rawOutput;
    }

    private ErrorHandler errorHandler = null;

    @Override
    public void setErrorHandler(ErrorHandler handler) {
        this.errorHandler = handler;
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    @Override
    public void parse(InputSource input) throws IOException, SAXException {
        run();
    }

    @Override
    public void parse(String systemId) throws IOException, SAXException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setContentHandler(ContentHandler ch) {
        assignOutput(ch);
    }

    private void assignOutput(ContentHandler ch) {
        rawOutput = ch;
        if (dumpFile != null) {
            DumpingLexicalXMLFilter dumper = new DumpingLexicalXMLFilter();
            dumper.setDumpFile(dumpFile);
            dumper.setContentHandler(ch);
            output = dumper;
        } else {
            output = ch;
        }
    }

    private Boolean aggregating = null;

    private boolean initialzed = false;
    
    private static final ThreadFactory DAEMON_THREAD_FACTORY = new ThreadFactory() {

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        }
    };
    
    /**
     * 
     * @return true if node has children
     */
    private boolean init() {
        if (initialzed) {
            return !nodes.isEmpty();
        }
        initialzed = true;
        if (nodes.isEmpty()) {
            if (inputFilter == null) {
                throw new IllegalStateException();
            } else {
                synchronized (this) {
                    assignOutput(inputFilter);
                    notify();
                }
            }
            return false;
        } else {
            if (executor == null) {
                setExecutor(Executors.newCachedThreadPool(DAEMON_THREAD_FACTORY));
            }
            if (output == null) {
                synchronized (this) {
                    StatefulXMLFilter sxf = new StatefulXMLFilter(DEPTH_LIMIT);
                    sxf.setName(name+"Output");
                    assignOutput(sxf);
                    notify();
                }
            }
            for (IdQueryable node : nodes) {
                ((IntegratorOutputNode) node).setExecutor(executor);
            }
            if (inputFilter != null) {
                names.addFirst(null);
                nodes.addFirst(inputFilter);
                requires.addFirst(true);
                if (aggregating == null) {
                    aggregating = false;
                }
            } else {
                if (aggregating == null) {
                    aggregating = nodes.size() == 1;
                }
            }
            int size = nodes.size();
            childNodes = nodes.toArray(new IdQueryable[size]);
            childElementNames = names.toArray(new String[size]);
            requireForWrite = requires.toArray(new Boolean[size]);
            for (IdQueryable idq : childNodes) {
                if (idq instanceof XMLReader) {
                    try {
                        ((XMLReader) idq).setProperty(SAXProperties.EXECUTOR_SERVICE_PROPERTY_NAME, executor);
                    } catch (SAXNotRecognizedException ex) {
                        logger.trace("ignoring "+ex);
                    } catch (SAXNotSupportedException ex) {
                        logger.trace("ignoring "+ex);
                    }
                }
            }
            return true;
        }
    }
    
    private ExecutorService executor;
    
    private JobMonitor<Void> childJobMonitor;
    
    private Future<?> childJobFuture;
    
    public ExecutorService getExecutor() {
        return executor;
    }
    
    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
        this.childJobMonitor = new JobMonitor<Void>(executor);
    }
    
    private static class JobMonitor<T> implements Runnable {

        private Collection<? extends Runnable> jobs;
        private final BlockingQueue<Future<T>> jobQueue = new LinkedBlockingQueue<Future<T>>();
        private final Collection<Future<T>> activeJobs = new HashSet<Future<T>>();
        private final ExecutorCompletionService<T> backing;
        private Throwable upstreamThrowable;
        private Thread target;

        public JobMonitor(ExecutorService backing) {
            this.backing = new ExecutorCompletionService<T>(backing, jobQueue);
        }
        
        public void init(Collection<? extends Runnable> jobs, Thread target) {
            this.jobs = jobs;
            this.target = target;
        }

        private void invokeJobs() {
            for (Runnable r : jobs) {
                activeJobs.add(backing.submit(r, null));
            }
        }
        
        private void join() throws InterruptedException, ExecutionException {
            while (!activeJobs.isEmpty()) {
                Future<T> job = jobQueue.take();
                try {
                    job.get();
                    activeJobs.remove(job);
                } catch (ExecutionException ex) {
                    activeJobs.remove(job);
                    drainActiveJobs();
                    throw ex;
                }
            }
        }

        private void drainActiveJobs() throws InterruptedException {
            int remaining = activeJobs.size();
            for (Future<T> remainingJob : activeJobs) {
                remainingJob.cancel(true);
            }
            for (int i = 0; i < remaining; i++) {
                activeJobs.remove(jobQueue.take());
            }
            target.interrupt();
        }

        @Override
        public void run() {
            try {
                try {
                    invokeJobs();
                    join();
                } catch (InterruptedException ex) {
                    upstreamThrowable = ex;
                    drainActiveJobs();
                    throw new RuntimeException(ex);
                } catch (ExecutionException ex) {
                    upstreamThrowable = ex.getCause();
                    drainActiveJobs();
                    throw new RuntimeException(ex);
                } catch (Throwable t) {
                    drainActiveJobs();
                    throw new RuntimeException(t);
                }
            } catch (InterruptedException ex) {
                if (upstreamThrowable == null) {
                    upstreamThrowable = ex;
                    throw new RuntimeException(ex);
                }
            }
        }

    };

    @Override
    public void run() {
        if (!init()) {
            inputFilter.run();
        } else {
            for (int i = 0; i < childNodes.length; i++) {
//                String setThreadName;
//                if (childElementNames[i] != null) {
//                    childNodes[i].setName(childElementNames[i]);
//                    setThreadName = childElementNames[i]+"<-"+Thread.currentThread().getName();
//                } else {
//                    setThreadName = childNodes[i].getName()+"<-"+Thread.currentThread().getName();
//                }
                if (requireForWrite[i]) {
                    requiredIndexes.add(i);
                }
            }
            childJobMonitor.init(Arrays.asList(childNodes), Thread.currentThread());
            childJobFuture = executor.submit(childJobMonitor);
            try {
                if (childNodes.length <= Integer.SIZE) {
                    int requiredIndexesBitflags = 0;
                    for (int i : requiredIndexes) {
                        requiredIndexesBitflags |= bitMasks[i];
                    }
                    run2(requiredIndexesBitflags);
                } else {
                    run2();
                }
                childJobFuture.get();
            } catch (SAXException ex) {
                handleLocalException();
                throw new RuntimeException(ex);
            } catch (Throwable t) {
                handleLocalException();
                throw new RuntimeException(t);
            }
        }
    }

    private void handleLocalException() {
        if (childJobMonitor.upstreamThrowable != null) {
            Throwable t = childJobMonitor.upstreamThrowable;
            if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            } else if (t instanceof ExecutionException) {
                throw new RuntimeException(t.getCause());
            } else {
                throw new RuntimeException(t);
            }
        } else {
            childJobFuture.cancel(true);
        }
    }
    
    private final LinkedHashSet<Integer> requiredIndexes = new LinkedHashSet<Integer>();

    private static XMLReader getXR() {
        try {
            return spf.newSAXParser().getXMLReader();
        } catch (ParserConfigurationException ex) {
            throw new RuntimeException(ex);
        } catch (SAXException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    public static void main(String[] args) throws Exception {
        IntegratorOutputNode root = new IntegratorOutputNode();
        boolean testProblem = true;
        if (testProblem) {
            root.addDescendent("/record/marc", new InputSourceXMLReader(getXR(), new InputSource("./src/test/resources/input/test-problem/bib.xml")), false);
            root.addDescendent("/record/holdings/holding", new InputSourceXMLReader(getXR(), new InputSource("./src/test/resources/input/test-problem/mfhd.xml")), false);
            root.addDescendent("/record/holdings/holding/items/item", new InputSourceXMLReader(getXR(), new InputSource("./src/test/resources/input/test-problem/item.xml")), false);
        } else {
            root.addDescendent("/record/marc", new InputSourceXMLReader(getXR(), new InputSource("./src/test/resources/input/real/marc.xml")), false);
            root.addDescendent("/record/holdings/holding", new InputSourceXMLReader(getXR(), new InputSource("./src/test/resources/input/real/hldg.xml")), false);
            root.addDescendent("/record/holdings/holding/items/item", new InputSourceXMLReader(getXR(), new InputSource("./src/test/resources/input/real/itemAll.xml")), false);
            root.addDescendent("/record/holdings/holding/items/item/itemStatuses/itemStatus", new InputSourceXMLReader(getXR(), new InputSource("./src/test/resources/input/real/itemStatus.xml")), false);
        }
//        root.addDescendent("/items/item", new PreConfiguredXMLReader(new InputSource("./src/test/resources/input/real/item.xml")), false);
//        root.addDescendent("/items/item/itemStatuses/itemStatus", new PreConfiguredXMLReader(new InputSource("./src/test/resources/input/real/itemStatus.xml")), false);
        ExecutorService executor = Executors.newCachedThreadPool();
        root.setProperty(SAXProperties.EXECUTOR_SERVICE_PROPERTY_NAME, executor);
        SAXTransformerFactory tf = (SAXTransformerFactory) TransformerFactory.newInstance("net.sf.saxon.TransformerFactoryImpl", null);
        Transformer t = tf.newTransformer();
        t.setOutputProperty(OutputKeys.INDENT, "yes");
        one(t, root, "/tmp/output.xml");
        root.reset();
        t.reset();
        t.setOutputProperty(OutputKeys.INDENT, "yes");
        one(t, root, "/tmp/output2.xml");
        executor.shutdown();
        System.err.println("DONE!");
    }

    private static void one(Transformer t, IntegratorOutputNode root, String systemId) throws TransformerException {
        t.transform(new SAXSource(root, new InputSource()), new StreamResult(systemId));
    }

    private static void two(Transformer t, IntegratorOutputNode root) throws TransformerException, SAXException {
        UnboundedContentHandlerBuffer out = new UnboundedContentHandlerBuffer();
        Exception e = null;
        try {
            t.transform(new SAXSource(root, new InputSource()), new SAXResult(out));
        } catch (Exception ex) {
            e = ex;
        }
        out.dump(System.out, false);
        if (e != null) {
            e.printStackTrace(System.out);
        }
    }

    private void run2() throws SAXException {
        ArrayList<Integer> activeIndexes = new ArrayList<Integer>();
        int lastOutputIndex = -1;
        int dipLevel = 0;
        boolean requiredInputExhausted = false;
        boolean allFinished = false;
        int[] levels = new int[childNodes.length];
        while (!allFinished && !requiredInputExhausted) {
            int level = -1;
            Comparable leastId = null;
            activeIndexes.clear();
            allFinished = true;

            // Get active level
            for (int i = 0; i < levels.length; i++) {
                try {
                    levels[i] = childNodes[i].getLevel();
                    if (levels[i] > level) {
                        level = levels[i];
                    }
                } catch (EOFException ex) {
                    levels[i] = -1;
                    if (requiredIndexes.contains(i)) {
                        requiredInputExhausted = true;
                    }
                }
            }
            if (level < dipLevel) {
                dipLevel = level;
            }

            // Get active indexes and active (least) id
            boolean leastIdExplicitlySet = false;
            for (int i = 0; i < childNodes.length; i++) {
                try {
                    if (levels[i] == level) {
                        Comparable tmpId = childNodes[i].getId();
                        if (!leastIdExplicitlySet || (leastId != null && tmpId != null && tmpId.compareTo(leastId) < 0)) {
                            leastIdExplicitlySet = true;
                            activeIndexes.clear();
                            activeIndexes.add(i);
                            leastId = tmpId;
                        } else if ((leastId == null && tmpId == null) || tmpId.compareTo(leastId) == 0) { //tmpId should never be null.
                            activeIndexes.add(i);
                        }
                    }
                    allFinished = false;
                } catch (EOFException ex) {
                    // ok.
                }
            }

            // Skip, write output, or step to next level
            if (!activeIndexes.containsAll(requiredIndexes)) {
                for (int i : activeIndexes) {
                    childNodes[i].skipOutput();
                }
                if (requiredInputExhausted) {
                    for (int i = 0; i < childNodes.length; i++) {
                        while (!childNodes[i].isFinished()) {
                            childNodes[i].skipOutput();
                        }
                    }
                }
            } else {
                Boolean self = null;
                for (int i : activeIndexes) {
                    if (childNodes[i].self()) {
                        if (self == null) {
                            self = true;
                            // write structural events once for current leastId
                            if (lastOutputIndex >= 0) {
                                childNodes[lastOutputIndex].writeEndElements(output, dipLevel, aggregating);
                            }
                            childNodes[i].writeStartElements(output, dipLevel, aggregating);
                            dipLevel = level; // reset to selfLevel (highest)
                        } else if (!self) {
                            throw new IllegalStateException("inconsistent child selfness for " + childElementNames[i] + ", asserted level=" + level + ", id=" + leastId);
                        }
                        if (childElementNames[i] != null) {
                            attRunner.clear();
                            attRunner.addAttribute("", "id", "id", "CDATA", leastId.toString());
                            output.startElement("", childElementNames[i], childElementNames[i], attRunner);
                        }
                        childNodes[i].writeOutput(output);
                        if (childElementNames[i] != null) {
                            output.endElement("", childElementNames[i], childElementNames[i]);
                        }
                        lastOutputIndex = i; // an index to get endElement events from.
                    } else {
                        if (self == null) {
                            self = false;
                        } else if (self) {
                            throw new IllegalStateException("inconsistent child selfness for " + childElementNames[i] + ", asserted level=" + level + ", id=" + leastId);
                        }
                        if (!childNodes[i].isFinished()) {
                            childNodes[i].step();
                        }
                    }
                }
            }
        }
        if (lastOutputIndex < 0) {
            childNodes[0].writeRootElement(output);
        } else {
            childNodes[lastOutputIndex].writeEndElements(output, 0, aggregating);
        }
    }

    private static final int[] bitMasks = new int[Integer.SIZE];
    private static final int[] notBitMasks = new int[Integer.SIZE];

    static {
        for (int i = 0; i < Integer.SIZE; i++) {
            bitMasks[i] = 0x80000000 >>> i;
            notBitMasks[i] = ~bitMasks[i];
        }
    }

    /**
     * DO NOT MODIFY THIS METHOD!  It should be a derivative of the run2()
     * reference implementation method! 
     * @param requiredIndexesBitflags
     * @throws SAXException
     */
    private void run2(int requiredIndexesBitflags) throws SAXException {
        int lastOutputIndex = -1;
        int dipLevel = 0;
        boolean requiredInputExhausted = false;
        boolean allFinished = false;
        int[] levels = new int[childNodes.length];
        while (!allFinished && !requiredInputExhausted) {
            int level = -1;
            Comparable leastId = null;
            int activeIndexesBitflags = 0;
            allFinished = true;

            // Get active level
            for (int i = 0; i < levels.length; i++) {
                try {
                    levels[i] = childNodes[i].getLevel();
                    if (levels[i] > level) {
                        level = levels[i];
                    }
                } catch (EOFException ex) {
                    levels[i] = -1;
                    if ((requiredIndexesBitflags & bitMasks[i]) != 0) {
                        requiredInputExhausted = true;
                    }
                }
            }
            if (level < dipLevel) {
                dipLevel = level;
            }

            // Get active indexes and active (least) id
            boolean leastIdExplicitlySet = false;
            for (int i = 0; i < childNodes.length; i++) {
                try {
                    if (levels[i] == level) {
                        Comparable tmpId = childNodes[i].getId();
                        if (!leastIdExplicitlySet || (leastId != null && tmpId != null && tmpId.compareTo(leastId) < 0)) {
                            leastIdExplicitlySet = true;
                            activeIndexesBitflags = 0;
                            activeIndexesBitflags |= bitMasks[i];
                            leastId = tmpId;
                        } else if ((leastId == null && tmpId == null) || tmpId.compareTo(leastId) == 0) { //tmpId should never be null.
                            activeIndexesBitflags |= bitMasks[i];
                        }
                    }
                    allFinished = false;
                } catch (EOFException ex) {
                    // ok.
                }
            }

            // Skip, write output, or step to next level
            if ((requiredIndexesBitflags & activeIndexesBitflags) != requiredIndexesBitflags) {
                int copy = activeIndexesBitflags; // for loop over activeIndexes
                for (int i = Integer.numberOfLeadingZeros(copy); i != Integer.SIZE; i = Integer.numberOfLeadingZeros(copy &= notBitMasks[i])) {
                    childNodes[i].skipOutput();
                }
                if (requiredInputExhausted) {
                    for (int i = 0; i < childNodes.length; i++) {
                        while (!childNodes[i].isFinished()) {
                            childNodes[i].skipOutput();
                        }
                    }
                }
            } else {
                Boolean self = null;
                int copy = activeIndexesBitflags; // for loop over activeIndexes
                for (int i = Integer.numberOfLeadingZeros(copy); i != Integer.SIZE; i = Integer.numberOfLeadingZeros(copy &= notBitMasks[i])) {
                    if (childNodes[i].self()) {
                        if (self == null) {
                            self = true;
                            // write structural events once for current leastId
                            if (lastOutputIndex >= 0) {
                                childNodes[lastOutputIndex].writeEndElements(output, dipLevel, aggregating);
                            }
                            childNodes[i].writeStartElements(output, dipLevel, aggregating);
                            dipLevel = level; // reset to selfLevel (highest)
                        } else if (!self) {
                            throw new IllegalStateException("inconsistent child selfness for " + childElementNames[i] + ", asserted level=" + level + ", id=" + leastId);
                        }
                        if (childElementNames[i] != null) {
                            attRunner.clear();
                            attRunner.addAttribute("", "id", "id", "CDATA", leastId.toString());
                            output.startElement("", childElementNames[i], childElementNames[i], attRunner);
                        }
                        childNodes[i].writeOutput(output);
                        if (childElementNames[i] != null) {
                            output.endElement("", childElementNames[i], childElementNames[i]);
                        }
                        lastOutputIndex = i; // an index to get endElement events from.
                    } else {
                        if (self == null) {
                            self = false;
                        } else if (self) {
                            throw new IllegalStateException("inconsistent child selfness for " + childElementNames[i] + ", asserted level=" + level + ", id=" + leastId);
                        }
                        if (!childNodes[i].isFinished()) {
                            childNodes[i].step();
                        }
                    }
                }
            }
        }
        if (lastOutputIndex < 0) {
            childNodes[0].writeRootElement(output);
        } else {
            childNodes[lastOutputIndex].writeEndElements(output, 0, aggregating);
        }
    }

    @Override
    public void step() {
        if (!debugging || rawOutput instanceof IdQueryable) {
            ((IdQueryable)rawOutput).step();
        }
    }

    public IntegratorOutputNode() {
        this.inputFilter = null;
    }

    public IntegratorOutputNode(StatefulXMLFilter payload) {
        if (payload != null) {
            this.inputFilter = payload;
        } else {
            this.inputFilter = null;
        }
    }

    @Override
    public boolean isFinished() {
        if (!debugging || rawOutput instanceof IdQueryable) {
            return ((IdQueryable)rawOutput).isFinished();
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public void skipOutput() {
        if (!debugging || rawOutput instanceof IdQueryable) {
            ((IdQueryable)rawOutput).skipOutput();
        }
    }

    @Override
    public void writeOutput(ContentHandler ch) {
        if (!debugging || rawOutput instanceof IdQueryable) {
            ((IdQueryable)rawOutput).writeOutput(ch);
        }
    }

    @Override
    public String buffersToString() {
        return ((StatefulXMLFilter) rawOutput).buffersToString(DEPTH_LIMIT);
    }

//    @Override
//    public void writeOuterStartElement(ContentHandler ch, boolean asSelf) {
//        if (!debugging || rawOutput instanceof IdQueryable) {
//            ((IdQueryable)rawOutput).writeOuterStartElement(ch, asSelf);
//        }
//    }
//
//    @Override
//    public void writeInnerStartElement(ContentHandler ch) {
//        if (!debugging || rawOutput instanceof IdQueryable) {
//            ((IdQueryable)rawOutput).writeInnerStartElement(ch);
//        }
//    }
//
//    @Override
//    public void writeInnerEndElement(ContentHandler ch) {
//        if (!debugging || rawOutput instanceof IdQueryable) {
//            ((IdQueryable)rawOutput).writeInnerEndElement(ch);
//        }
//    }
//
//    @Override
//    public void writeOuterEndElement(ContentHandler ch) {
//        if (!debugging || rawOutput instanceof IdQueryable) {
//            ((IdQueryable)rawOutput).writeOuterEndElement(ch);
//        }
//    }
//
    @Override
    public void writeEndElements(ContentHandler ch, int lowerLevel, boolean aggregate) throws SAXException {
        if (!debugging || rawOutput instanceof IdQueryable) {
            ((IdQueryable)rawOutput).writeEndElements(ch, lowerLevel, aggregate);
        }
    }

    @Override
    public void writeStartElements(ContentHandler ch, int lowerLevel, boolean aggregate) throws SAXException {
        if (!debugging || rawOutput instanceof IdQueryable) {
            ((IdQueryable)rawOutput).writeStartElements(ch, lowerLevel, aggregate);
        }
    }

    @Override
    public void writeRootElement(ContentHandler ch) throws SAXException {
        if (!debugging || rawOutput instanceof IdQueryable) {
            ((IdQueryable)rawOutput).writeRootElement(ch);
        }
    }

    private AttributesImpl attRunner = new AttributesImpl();

    @Override
    public boolean self() {
        blockForOutputFilterInitialization();
        if (!debugging || rawOutput instanceof IdQueryable) {
            return ((IdQueryable)rawOutput).self();
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public Comparable getId() throws EOFException {
        blockForOutputFilterInitialization();
        if (!debugging || rawOutput instanceof IdQueryable) {
            return ((IdQueryable)rawOutput).getId();
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public int getLevel() throws EOFException {
        blockForOutputFilterInitialization();
        if (!debugging || rawOutput instanceof IdQueryable) {
            return ((IdQueryable)rawOutput).getLevel();
        } else {
            throw new IllegalStateException();
        }
    }

    private void blockForOutputFilterInitialization() {
        synchronized (this) {
            while (output == null) {
                notify();
                try {
                    wait();
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }

    public void addChild(String childElementName, IdQueryable child, boolean requireForWrite) {
        if (childElementName == null || child == null) {
            throw new NullPointerException();
        }
        names.add(childElementName);
        child.setName(childElementName);
        nodes.add(child);
        requires.add(requireForWrite);
    }

    private final LinkedList<String> names = new LinkedList<String>();
    private final LinkedList<IdQueryable> nodes = new LinkedList<IdQueryable>();
    private final LinkedList<Boolean> requires = new LinkedList<Boolean>();


    private static class PreConfiguredXMLReader extends XMLFilterImpl {

        private final InputSource in;

        private PreConfiguredXMLReader(InputSource in) throws ParserConfigurationException, SAXException {
            this.in = in;
            SAXParserFactory spf = SAXParserFactory.newInstance();
            spf.setNamespaceAware(true);
            SAXParser sp = spf.newSAXParser();
            setParent(sp.getXMLReader());
        }

        @Override
        public void parse(InputSource input) throws SAXException, IOException {
            super.parse(in);
        }

        @Override
        public void parse(String systemId) throws SAXException, IOException {
            super.parse(in);
        }

    }

}
