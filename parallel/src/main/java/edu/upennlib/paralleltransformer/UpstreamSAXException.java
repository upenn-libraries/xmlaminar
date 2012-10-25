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

package edu.upennlib.paralleltransformer;

import org.xml.sax.SAXException;

/**
 *
 * @author Michael Gibney
 */
public class UpstreamSAXException extends SAXException {

    private final SAXException source;

    public UpstreamSAXException(InterruptedException immediate, SAXException source) {
        super(immediate);
        this.source = source;
    }

    @Override
    public Exception getException() {
        return source.getException();
    }

    @Override
    public String getMessage() {
        return source.getMessage();
    }

    @Override
    public String toString() {
        return source.toString();
    }

}
