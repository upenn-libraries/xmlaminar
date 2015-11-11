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

package edu.upenn.library.xmlaminar.solr;

import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

/**
 *
 * @author michael
 */
public class SolrServerFactory {

    private String solrURL;
    private ResponseParser responseParser;

    public String getSolrURL() {
        return solrURL;
    }

    public void setSolrURL(String solrURL) {
        this.solrURL = solrURL;
    }

    public ResponseParser getResponseParser() {
        return responseParser;
    }

    public void setResponseParser(ResponseParser parser) {
        responseParser = parser;
    }

    public SolrServer getServer() {
        SolrServer s = getServerInstance();
        if (responseParser != null) {
            if (s instanceof ConcurrentUpdateSolrServer) {
                ((ConcurrentUpdateSolrServer)s).setParser(responseParser);
            } else if (s instanceof HttpSolrServer) {
                ((HttpSolrServer)s).setParser(responseParser);
            }

        }
        return s;
    }

    protected SolrServer getServerInstance() {
        return new HttpSolrServer(getSolrURL());
    }

}
