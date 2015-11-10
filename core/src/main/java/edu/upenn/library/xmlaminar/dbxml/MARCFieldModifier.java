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

package edu.upenn.library.xmlaminar.dbxml;

import java.nio.CharBuffer;

/**
 *
 * @author michael
 */
public interface MARCFieldModifier {
    /**
     * For modifying fields from MARC records.
     * @param tag The marc field number
     * @param original The contents of the original field (indicators, subfields,
     * content, through field terminator (inclusive).
     * @param output The destination array intended to contain the modified field.
     * @param startAndEnd A two-element array of start (inclusive) and end (exclusive)
     * indices of the modified field content placed in the output array.
     * @return The output array containing the modified field contents. If the
     * specified output array argument is not large enough to contain the modified
     * field contents, a new array will be returned (a la Collection.toArray())
     */
    public char[] modifyField(String tag, CharBuffer original, char[] output, int[] startAndEnd);
}
