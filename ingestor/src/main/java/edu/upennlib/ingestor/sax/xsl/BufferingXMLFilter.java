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

package edu.upennlib.ingestor.sax.xsl;

import edu.upennlib.ingestor.sax.utils.MyXFI;
import edu.upennlib.ingestor.sax.utils.NoopXMLFilter;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Queue;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;
import org.xml.sax.helpers.XMLFilterImpl;
/**
 *
 * @author michael
 */
public class BufferingXMLFilter extends MyXFI {

    public static final String TRANSFORMER_FACTORY_CLASS_NAME = "net.sf.saxon.TransformerFactoryImpl";
    public static final int QUEUE_ARRAY_SIZE = 2000;
    public static final boolean USE_LINKED_LIST = false;
    private SaxEventExecutor executor = new SaxEventExecutor();

    private final Queue[] eventQueue = new Queue[1];
    private int queueSize = 0;
    private int queueThreshold = -1;
    private int queueSizeLimit = -1;
    private boolean parsing = false;
    private Thread eventPlayer = null;
    boolean verbose = false;

    public static void main(String[] args) throws TransformerConfigurationException, TransformerException, ParserConfigurationException, SAXException, IOException, InterruptedException {
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        SAXParser parser = spf.newSAXParser();
        TransformerFactory tf = TransformerFactory.newInstance(TRANSFORMER_FACTORY_CLASS_NAME, null);
        Transformer t = tf.newTransformer();
        BufferingXMLFilter instance = new BufferingXMLFilter();
        instance.setParent(parser.getXMLReader());
        long start = System.currentTimeMillis();
        InputSource input = new InputSource("/tmp/large.xml");
        boolean playable = true;
        if (playable) {
            instance.getParent().setContentHandler(instance);
            instance.getParent().setProperty("http://xml.org/sax/properties/lexical-handler", instance);
            instance.getParent().parse(input);
            instance.play(new MyContentHandler());
            synchronized(instance){
                instance.wait(5000);
            }
            System.out.println("in between");
            instance.play(new MyContentHandler());
        } else {
            FileOutputStream fos = new FileOutputStream("/tmp/bufferedWorking.xml");
            BufferedOutputStream bos = new BufferedOutputStream(fos);
            t.transform(new SAXSource(instance, input), new StreamResult(bos));
            bos.close();
        }
        System.out.println("duration: " + (System.currentTimeMillis() - start));
    }

    public BufferingXMLFilter() {
        charArray = SaxEventExecutor.getNextCharArray();
        charBuffer = CharBuffer.wrap(charArray);
    }

    private static class MyContentHandler extends XMLFilterImpl {

        int index = 0;
        @Override
        public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
            if (qName.equals("marc:record")) {
                System.out.println(++index);
            }
        }
    }

    @Override
    public void parse(InputSource input) throws SAXException, IOException {
        if (eventPlayer != null) {
            throw new IllegalStateException();
        }
        if (getParent() instanceof NoopXMLFilter) {
            replayableLocal(getContentHandler(), true, true);
        } else {
            setQueueSizeLimit(QUEUE_ARRAY_SIZE);
            parsing = true;
            eventPlayer = new Thread(new EventPlayer(), "bufferPlayerThread");
            eventPlayer.start();
            super.parse(input);
            parsing = false;
            synchronized (eventQueue) {
                eventQueue.notify();
            }
            try {
                eventPlayer.join();
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
            eventPlayer = null;
        }
    }

    public void clear() {
        queueSize = 0;
        if (eventQueue[0] != null) {
            eventQueue[0].clear();
        }
    }

    @Override
    public void parse(String systemId) throws SAXException, IOException {
        throw new UnsupportedOperationException();
    }

    private void setQueueSizeLimit(int queueSizeLimit) {
        if (queueSizeLimit < 1) {
            throw new IllegalArgumentException();
        }
        this.queueSizeLimit = queueSizeLimit;
        queueThreshold = (int) Math.ceil((double) queueSizeLimit / 2);
        if (eventQueue[0] != null) {
            throw new IllegalStateException();
        } else {
            if (USE_LINKED_LIST) {
                eventQueue[0] = new LinkedList<Object[]>();
            } else {
                eventQueue[0] = new MyBoundedQueue<Object[]>(queueSizeLimit);
            }
        }
    }

    private boolean bufferSaxEvent(Object... args) {
        if (eventQueue[0] == null) {
            if (USE_LINKED_LIST) {
                eventQueue[0] = new LinkedList<Object[]>();
            } else {
                eventQueue[0] = new MyUnboundedQueue<Object[]>(QUEUE_ARRAY_SIZE);
            }
        }
        synchronized (eventQueue) {
            if (queueSizeLimit != -1 && queueSize >= queueSizeLimit) {
                try {
                    eventQueue.wait();
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
            if (args[0] == SaxEventType.characters || args[0] == SaxEventType.ignorableWhitespace) {
                char[] chOrig = (char[]) args[1];
                addCharacters(args); // modifies args.
                if (chOrig.equals(args[1])) {
                    throw new RuntimeException("not working properly");
                }
            } else if (args[0] == SaxEventType.startElement) {
                args[4] = new AttributesImpl((Attributes) args[4]);
            }
            //System.out.println(Arrays.asList(args));
            eventQueue[0].add(args);
            queueSize++;
            eventQueue.notify();
            return true;
        }
    }

    private void addCharacters(Object[] args) {
        char[] chOrig = (char[]) args[1];
        int offsetOrig = (Integer) args[2];
        int length = (Integer) args[3];
        if (charBuffer.remaining() < length) {
            charArray = SaxEventExecutor.getNextCharArray(length);
            charBuffer = CharBuffer.wrap(charArray);
        }
        args[1] = charArray;
        args[2] = charBuffer.position();
        charBuffer.put(chOrig, offsetOrig, length);
    }

    private char[] charArray;
    private CharBuffer charBuffer;

    public int play(ContentHandler ch) throws SAXException {
        if (eventPlayer != null) {
            throw new IllegalStateException();
        }
        return replayableLocal(ch, true, true);
    }

    public int flush(ContentHandler ch) throws SAXException {
        if (eventPlayer != null) {
            throw new IllegalStateException();
        }
        return playLocal(ch, true, true);
    }

    public int playMostRecentStructurallyInsignificant(ContentHandler ch) throws SAXException {
        Iterator<Object[]> iter = reverseIterator();
        int level = 0;
        while (iter.hasNext()) {
            Object[] next = iter.next();
            if (SaxEventExecutor.isStructurallySignificant(next)) {
                break;
            }
            level += executor.executeSaxEvent(ch, next, true, true);
        }
        return level;
    }

    private int playLocal(ContentHandler ch, boolean writeStructural, boolean writeNonStructural) throws SAXException {
        Object[] next = null;
        int level = 0;
        synchronized (eventQueue) {
            if (eventQueue[0] != null && !eventQueue[0].isEmpty()) {
                next = (Object[]) eventQueue[0].remove();
                queueSize--;
            }
        }
        while (next != null) {
            //System.out.println("on:"+ch+", "+Arrays.asList(next));
            level += executor.executeSaxEvent(ch, next, writeStructural, writeNonStructural);
            synchronized (eventQueue) {
                if (queueSize != 0) {
                    next = (Object[]) eventQueue[0].remove();
                    queueSize--;
                    if (queueSizeLimit != -1 && queueSize < queueThreshold) {
                        eventQueue.notify();
                    }
                } else {
                    next = null;
                }
            }
        }
        return level;
    }

    private int replayableLocal(ContentHandler ch, boolean writeStructural, boolean writeNonStructural) throws SAXException {
        if (eventQueue[0] == null) {
            return 0;
        } else {
            int level = 0;
            synchronized (eventQueue) {
                Iterator iter = eventQueue[0].iterator();
                while (iter.hasNext()) {
                    Object[] next = (Object[]) iter.next();
                    if (ch == null) {
                        if (next[0] == SaxEventType.startElement) {
                            System.out.println(hashCode()+"\t"+next[0]+", "+next[1]+", "+next[2]+", "+next[3]+", "+attsToString((Attributes)next[4]));
                        } else {
                            System.out.println(hashCode()+"\t"+Arrays.asList(next));
                        }
                    } else {
                        level += executor.executeSaxEvent(ch, next, writeStructural, writeNonStructural);
                    }
                }
            }
            return level;
        }
    }

    private static String attsToString(Attributes atts) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < atts.getLength(); i++) {
            sb.append(atts.getURI(i)).append(atts.getQName(i)).append("=\"").append(atts.getValue(i)).append("\", ");
        }
        sb.append(atts.getClass().getCanonicalName());
        return sb.toString();
    }

    public Iterator iterator() {
        if (eventQueue[0] == null) {
            return new Iterator() {

                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public Object next() {
                    throw new UnsupportedOperationException("Not supported yet.");
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("Not supported yet.");
                }
            };
        }
        return eventQueue[0].iterator();
    }

    public Iterator reverseIterator() {
        if (eventQueue[0] == null) {
            throw new IllegalStateException();
        } else if (eventQueue[0] instanceof MyUnboundedQueue) {
            return ((MyUnboundedQueue)eventQueue[0]).reverseIterator();
        } else if (eventQueue[0] instanceof MyBoundedQueue) {
            return ((MyBoundedQueue)eventQueue[0]).reverseIterator();
        } else if (eventQueue[0] instanceof LinkedList) {
            return new LinkedListReverseIterator((LinkedList)eventQueue[0]);
        } else {
            throw new RuntimeException();
        }
    }

    private class LinkedListReverseIterator implements Iterator {

        ListIterator listIterator;

        public LinkedListReverseIterator(List list) {
            listIterator = list.listIterator(list.size());
        }

        @Override
        public boolean hasNext() {
            return listIterator.hasPrevious();
        }

        @Override
        public Object next() {
            return listIterator.previous();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Not supported.");
        }

    }

    @Override
    public void startDocument() throws SAXException {
        bufferSaxEvent(SaxEventType.startDocument);
    }

    @Override
    public void endDocument() throws SAXException {
        bufferSaxEvent(SaxEventType.endDocument);
    }

    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
        bufferSaxEvent(SaxEventType.startPrefixMapping, prefix, uri);
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
        bufferSaxEvent(SaxEventType.endPrefixMapping, prefix);
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        bufferSaxEvent(SaxEventType.startElement, uri, localName, qName, atts);
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        bufferSaxEvent(SaxEventType.endElement, uri, localName, qName);
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        bufferSaxEvent(SaxEventType.characters, ch, start, length);
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        bufferSaxEvent(SaxEventType.ignorableWhitespace, ch, start, length);
    }

    @Override
    public void processingInstruction(String target, String data) throws SAXException {
        bufferSaxEvent(SaxEventType.processingInstruction, target, data);
    }

    @Override
    public void skippedEntity(String name) throws SAXException {
        bufferSaxEvent(SaxEventType.skippedEntity, name);
    }

    @Override
    public void comment(char[] ch, int start, int length) throws SAXException {
        if (verbose) {
            System.out.println("buffering: "+Arrays.asList(SaxEventType.comment, ch, start, length));
        }
        bufferSaxEvent(SaxEventType.comment, ch, start, length);
    }

    @Override
    public void endCDATA() throws SAXException {
        if (verbose) {
            System.out.println(getParent()+" buffering: "+Arrays.asList(SaxEventType.endCDATA));
        }
        bufferSaxEvent(SaxEventType.endCDATA);
    }

    @Override
    public void endDTD() throws SAXException {
        if (verbose) {
            System.out.println(getParent()+" buffering: "+Arrays.asList(SaxEventType.endDTD));
        }
        bufferSaxEvent(SaxEventType.endDTD);
    }

    @Override
    public void endEntity(String name) throws SAXException {
        if (verbose) {
            System.out.println(getParent()+" buffering: "+Arrays.asList(SaxEventType.endEntity, name));
        }
        bufferSaxEvent(SaxEventType.endEntity, name);
    }

    @Override
    public void startCDATA() throws SAXException {
        if (verbose) {
            System.out.println(getParent()+" buffering: "+Arrays.asList(SaxEventType.startCDATA));
        }
        bufferSaxEvent(SaxEventType.startCDATA);
    }

    @Override
    public void startDTD(String name, String publicId, String systemId) throws SAXException {
        if (verbose) {
            System.out.println(getParent()+" buffering: "+Arrays.asList(SaxEventType.startDTD, name, publicId, systemId));
        }
        bufferSaxEvent(SaxEventType.startDTD, name, publicId, systemId);
    }

    @Override
    public void startEntity(String name) throws SAXException {
        if (verbose) {
            System.out.println(getParent()+" buffering: "+Arrays.asList(SaxEventType.startEntity, name));
        }
        bufferSaxEvent(SaxEventType.startEntity, name);
    }

    private class EventPlayer implements Runnable {

        ContentHandler ch = getContentHandler();

        //@Override
        public void runOld() {
            while (parsing) {
                try {
                    playLocal(ch, true, true);
                } catch (SAXException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }

        @Override
        public void run() {
            Object[] next;
            do {
                next = null;
                synchronized (eventQueue) {
                    while (parsing && (eventQueue[0] == null || eventQueue[0].isEmpty())) {
                        try {
                            eventQueue.wait();
                        } catch (InterruptedException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                    if (eventQueue[0] != null && !eventQueue[0].isEmpty()) {
                        next = (Object[]) eventQueue[0].remove();
                        queueSize--;
                        if (queueSizeLimit != -1 && queueSize < queueThreshold) {
                            eventQueue.notify();
                        }
                    }
                }
                if (next != null) {
                    try {
                        executor.executeSaxEvent(ch, next, true, true);
                    } catch (SAXException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            } while (next != null);
        }

    }

}
