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

package edu.upennlib.ingestor.sax.integrator;

/**
 *
 * @author michael
 */
public final class IdUpenn implements Comparable {
    public final String idType;
    public final String idString;
    public final long idLong;
    public final boolean idTypeInterned;
    public IdUpenn(String idType, String idString, boolean idTypeInterned) {
        if (idType == null || idString == null) {
            throw new IllegalArgumentException();
        }
        this.idType = idType;
        this.idTypeInterned = idTypeInterned;
        this.idString = idString;
        this.idLong = Long.parseLong(idString);
    }

    @Override
    public final int compareTo(Object o) {
        IdUpenn other = (IdUpenn) o;
        if (idTypeInterned && other.idTypeInterned) {
            if (other.idType != idType) {
                if (!other.idType.equals(idType)) {
                    throw new IllegalArgumentException("attempt to compare IdUpenn ids of different types: " + other.idType + ", " + idType);
                } else {
                    throw new IllegalStateException("bad use of string interning! fix this in code.");
                }
            }
        } else if (!other.idType.equals(idType)) {
            throw new IllegalArgumentException("attempt to compare IdUpenn ids of different types: "+other.idType+", "+idType);
        }
        if (idLong > other.idLong) {
            return 1;
        } else if (idLong < other.idLong) {
            return -1;
        } else {
            return 0;
        }
    }

    @Override
    public String toString() {
        return idType+"-"+idString;
    }
}
