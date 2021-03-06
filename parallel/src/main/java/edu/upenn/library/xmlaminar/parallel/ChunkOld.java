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

package edu.upenn.library.xmlaminar.parallel;

import edu.upenn.library.xmlaminar.UnboundedContentHandlerBuffer;

/**
 *
 * @author Michael Gibney
 */
public class ChunkOld implements Comparable {

    private int sequenceId;
    private UnboundedContentHandlerBuffer in = new UnboundedContentHandlerBuffer();
    private UnboundedContentHandlerBuffer out = new UnboundedContentHandlerBuffer();

    boolean precedes(ChunkOld other) {
        return (sequenceId ^ other.sequenceId) < 0 ^ Math.abs(sequenceId) < Math.abs(other.sequenceId);
    }

    @Override
    public int compareTo(Object o) {
        ChunkOld other = (ChunkOld) o;
        if ((sequenceId ^ other.sequenceId) < 0) {
            return Math.abs(other.sequenceId) - Math.abs(sequenceId);
        } else {
            return Math.abs(sequenceId) - Math.abs(other.sequenceId);
        }
    }

}
