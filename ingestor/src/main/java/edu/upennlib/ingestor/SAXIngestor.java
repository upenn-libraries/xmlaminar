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

package edu.upennlib.ingestor;

import edu.upennlib.ingestor.sax.integrator.IntegratorOutputNode;
import edu.upennlib.paralleltransformer.TXMLFilter;
import edu.upennlib.solrposter.SAXSolrPoster;
import edu.upennlib.xmlutils.DumpingLexicalXMLFilter;
import edu.upennlib.xmlutils.dbxml.PerformanceEvaluator;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.logging.Level;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.sax.SAXTransformerFactory;
import net.sf.saxon.Controller;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;

/**
 *
 * @author michael
 */
public class SAXIngestor implements Runnable {

    private String name = null;
    private IntegratorOutputNode integrator = null;
    private SAXSolrPoster solrPoster = null;
    private TXMLFilter joiner = null;
    private static final Logger logger = Logger.getLogger(SAXIngestor.class);
    private PerformanceEvaluator pe;
    private volatile boolean ingestSuccessful = false;
    public static final boolean DEFAULT_PREDELETE = false;
    public static final boolean DEFAULT_AUTOCOMMIT = true;
    public static final boolean DEFAULT_AUTOROLLBACK = true;
    private boolean preDelete = DEFAULT_PREDELETE;
    private boolean autoRollback = DEFAULT_AUTOROLLBACK;
    private boolean autoCommit = DEFAULT_AUTOCOMMIT;
    private URI solrServerURI;
    private DumpingLexicalXMLFilter dch;

    public void setDumpFile(File file) {
        if (file == null) {
            dch = null;
        } else {
            dch = new DumpingLexicalXMLFilter();
            dch.setDumpFile(file);
        }
    }

    public File getDumpFile() {
        return dch == null ? null : dch.getDumpFile();
    }
    
    public boolean isPreDelete() {
        return preDelete;
    }
    
    public void setPreDelete(boolean preDelete) {
        this.preDelete = preDelete;
    }
    
    public boolean isAutoCommit() {
        return autoCommit;
    }
    
    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }
    
    public boolean isAutoRollback() {
        return autoRollback;
    }
    
    public void setAutoRollback(boolean autoRollback) {
        this.autoRollback = autoRollback;
    }
    
    public URI getSolrServerURI() {
        return solrServerURI;
    }
    
    public void setSolrServerURI(URI uri) throws URISyntaxException {
        String path = uri.getPath();
        if (path.charAt(path.length() - 1) == '/') {
            solrServerURI = uri;
        } else {
            solrServerURI = new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(), uri.getPath().concat("/"), uri.getQuery(), uri.getFragment());
        }
    }
    
    public PerformanceEvaluator getPerformanceEvaluator() {
        return pe;
    }

    public void setPerformanceEvaluator(PerformanceEvaluator pe) {
        this.pe = pe;
    }

    public IntegratorOutputNode getIntegrator() {
        return integrator;
    }

    public void setIntegrator(IntegratorOutputNode integrator) {
        this.integrator = integrator;
    }

    public TXMLFilter getJoiner() {
        return joiner;
    }

    public void setJoiner(TXMLFilter joiner) {
        this.joiner = joiner;
    }

    public SAXSolrPoster getSolrPoster() {
        return solrPoster;
    }

    public void setSolrPoster(SAXSolrPoster solrPoster) {
        this.solrPoster = solrPoster;
    }

    @Override
    public void run() {
        logger.trace("run() called on " + getName());
        long start = System.currentTimeMillis();
        SAXTransformerFactory tf = (SAXTransformerFactory) TransformerFactory.newInstance("net.sf.saxon.TransformerFactoryImpl", null);
        Transformer t;
        try {
            t = tf.newTransformer();
        } catch (TransformerConfigurationException ex) {
            throw new RuntimeException(ex);
        }
        joiner.configureOutputTransformer((Controller) t);
        joiner.setParent(integrator);
        SolrServer server = solrPoster.getServer();
        if (autoRollback) {
            System.out.println("registering shutdown hook");
            Runtime.getRuntime().addShutdownHook(new RollbackShutdownHook(server, Thread.currentThread()));
        }
        try {
            if (preDelete) {
                logger.info("pre-deleting contents from solr server: "+server);
                server.deleteByQuery("*:*");
            }
            ContentHandler out;
            if (dch == null) {
                out = solrPoster;
            } else {
                dch.setContentHandler(solrPoster);
                out = dch;
            }
            t.transform(new SAXSource(joiner, new InputSource()), new SAXResult(out));
            if (autoCommit) {
                logger.info("auto-committing to solr server: " + solrPoster.getServer());
                server.commit();
            }
            ingestSuccessful = true;
        } catch (Throwable ex) {
            logger.fatal("Exception in ingest process");
            if (!autoRollback) {
                logger.fatal("No attempt to rollback changes; perform rollback manually.");
                solrPoster.shutdown();
            }
            throw new RuntimeException(ex);
        }
        solrPoster.shutdown();
        long processingStart = pe.getLastStart();
        long end = System.currentTimeMillis();
        long processingTime = end - processingStart;
        logger.info("Elapsed time: " + (end - start));
        logger.info("Processing time: " + processingTime);
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
    
    private class RollbackShutdownHook extends Thread {
        
        private final Thread main;
        private final SolrServer server;
        
        private RollbackShutdownHook(SolrServer server, Thread main) {
            this.server = server;
            this.main = main;
        }

        @Override
        public void run() {
            if (!ingestSuccessful) {
                logger.error("ingest not successful; attempting to rollback changes.");
                main.interrupt();
                try {
                    logger.error("waiting for main thread to die");
                    main.join();
                    logger.error("shutting down update server");
                    server.shutdown();
                    logger.error("issuing rollback command");
                    rollback(solrServerURI);
                } catch (Throwable ex) {
                    logger.error("attempt to rollback changes failed", ex);
                }
            }
        }
        
        public void rollback(URI uri) throws MalformedURLException, IOException, URISyntaxException {
            URL rollbackURL = new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(), uri.getPath().concat("update"), "stream.body=<rollback/>", uri.getFragment()).toURL();
            HttpURLConnection conn = (HttpURLConnection) rollbackURL.openConnection();
            conn.setInstanceFollowRedirects(true);
            conn.setConnectTimeout(120000);
            conn.setReadTimeout(120000);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            byte[] buff = new byte[1024];
            InputStream content = (InputStream) conn.getContent();
            int read;
            while ((read = content.read(buff)) != -1) {
                baos.write(buff, 0, read);
            }
            if (HttpURLConnection.HTTP_OK == conn.getResponseCode()) {
                logger.error("rollback successful: "+baos.toString("UTF-8"));
            } else {
                logger.error("rollback not successful: "+baos.toString("UTF-8"));
            }
        }

    }

}
