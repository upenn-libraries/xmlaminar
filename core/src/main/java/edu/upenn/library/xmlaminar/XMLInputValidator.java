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

package edu.upenn.library.xmlaminar;

import java.io.FilterReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;

/**
 *
 * @author Michael Gibney
 */
public class XMLInputValidator extends FilterReader {

    private static final char REPLACEMENT = Charset.forName("UTF-8").newDecoder().replacement().charAt(0);

    public XMLInputValidator(Reader wrapped) {
        super(wrapped);
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        int toReturn = super.read(cbuf, off, len);
        for (int i = off; i < off + len; i++) {
            if (!isValidXMLCharacter(cbuf[i])) {
                cbuf[i] = REPLACEMENT;
            }
        }
        return toReturn;
    }

    private static boolean isValidXMLCharacter(int c) {
        return (c >= 0x20 && c <= 0xD7FF) || (c >= 0xE000 && c <= 0xFFFD) || c == 0xD || c == 0xA || c == 0x9;
    }

    @Override
    public int read() throws IOException {
        int c = super.read();
        return (isValidXMLCharacter(c) ? c : REPLACEMENT);
    }
}
