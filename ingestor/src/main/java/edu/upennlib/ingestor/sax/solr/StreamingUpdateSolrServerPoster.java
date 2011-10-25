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

package edu.upennlib.ingestor.sax.solr;

import java.net.MalformedURLException;
import org.apache.solr.client.solrj.impl.StreamingUpdateSolrServer;

/**
 *
 * @author michael
 */
public class StreamingUpdateSolrServerPoster extends SAXSolrPoster<StreamingUpdateSolrServer> {

    private int queueSize;
    private int threadCount;

    public int getQueueSize() {
        return queueSize;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }
    
    public void initSpring() {
        try {
            setServer(new StreamingUpdateSolrServer(getSolrURL(), queueSize, threadCount));
        } catch (MalformedURLException ex) {
            throw new RuntimeException(ex);
        }
    }

}
