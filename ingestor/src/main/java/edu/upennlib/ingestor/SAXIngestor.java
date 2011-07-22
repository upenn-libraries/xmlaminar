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
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.upennlib.ingestor;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.logging.Level;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import org.apache.log4j.Logger;
import edu.upennlib.configurationutils.IndexedPropertyConfigurable;
import edu.upennlib.ingestor.sax.integrator.IntegratorOutputNode;
import edu.upennlib.ingestor.sax.solr.TrueSAXSolrPoster;
import edu.upennlib.ingestor.sax.xsl.JoiningXMLFilter;
import java.io.File;
import javax.xml.transform.sax.SAXResult;
import org.xml.sax.SAXException;

/**
 *
 * @author michael
 */
public class SAXIngestor implements Runnable, IndexedPropertyConfigurable {

    private String name = null;
    private IntegratorOutputNode integrator = null;
    private File stylesheet = null;
    private TrueSAXSolrPoster solrPoster = null;
    private JoiningXMLFilter joiner = null;
    private Logger logger = Logger.getLogger(getClass());


    public IntegratorOutputNode getIntegrator() {
        return integrator;
    }

    public void setIntegrator(IntegratorOutputNode integrator) {
        this.integrator = integrator;
    }

    public JoiningXMLFilter getJoiner() {
        return joiner;
    }

    public void setJoiner(JoiningXMLFilter joiner) {
        this.joiner = joiner;
    }

    public TrueSAXSolrPoster getSolrPoster() {
        return solrPoster;
    }

    public void setSolrPoster(TrueSAXSolrPoster solrPoster) {
        this.solrPoster = solrPoster;
    }

    public File getStylesheet() {
        return stylesheet;
    }

    public void setStylesheet(File stylesheet) {
        this.stylesheet = stylesheet;
    }
    @Override
    public void run() {
        logger.trace("run() called on "+getName());
        try {
            joiner.transform(integrator, stylesheet, new SAXResult(solrPoster));
        } catch (ParserConfigurationException ex) {
            throw new RuntimeException(ex);
        } catch (SAXException ex) {
            throw new RuntimeException(ex);
        } catch (FileNotFoundException ex) {
            throw new RuntimeException(ex);
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
