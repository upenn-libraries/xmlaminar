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

import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import org.apache.log4j.Logger;
import edu.upennlib.configurationutils.IndexedPropertyConfigurable;
import edu.upennlib.ingestor.sax.integrator.IntegratorOutputNode;
import edu.upennlib.paralleltransformer.TransformingXMLFilter;
import edu.upennlib.solrposter.SAXSolrPoster;
import edu.upennlib.xmlutils.dbxml.PerformanceEvaluator;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.sax.SAXTransformerFactory;
import net.sf.saxon.Controller;
import org.xml.sax.InputSource;

/**
 *
 * @author michael
 */
public class SAXIngestor implements Runnable, IndexedPropertyConfigurable {

    private String name = null;
    private IntegratorOutputNode integrator = null;
    private SAXSolrPoster solrPoster = null;
    private TransformingXMLFilter joiner = null;
    private static final Logger logger = Logger.getLogger(SAXIngestor.class);
    private PerformanceEvaluator pe;

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

    public TransformingXMLFilter getJoiner() {
        return joiner;
    }

    public void setJoiner(TransformingXMLFilter joiner) {
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
        logger.trace("run() called on "+getName());
        try {
            long start = System.currentTimeMillis();
            SAXTransformerFactory tf = (SAXTransformerFactory)TransformerFactory.newInstance(TransformingXMLFilter.TRANSFORMER_FACTORY_CLASS_NAME, null);
            Transformer t = tf.newTransformer();
            joiner.configureOutputTransformer((Controller) t);
            joiner.setStreamingParent(integrator);
            t.transform(new SAXSource(joiner, new InputSource()), new SAXResult(solrPoster));
            solrPoster.shutdown();
            long processingStart = pe.getLastStart();
            long end = System.currentTimeMillis();
            long processingTime = end - processingStart;
            System.out.println("Elapsed time: "+(end - start));
            System.out.println("Processing time: "+processingTime);
        } catch (TransformerConfigurationException ex) {
            throw new RuntimeException(ex);
        } catch (TransformerException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

}
