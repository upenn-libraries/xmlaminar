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
import java.util.Arrays;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

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
        sanitizeXMLCharacters(cbuf, off, toReturn);
        return toReturn;
    }

    public static boolean isValidXMLCharacter(int c) {
        return is16BitXMLCharacter(c) || isHighXMLCharacter(c);
    }

    public static boolean is16BitXMLCharacter(int c) {
        return (c >= 0x20 && c <= 0xD7FF) || (c >= 0xE000 && c <= 0xFFFD) 
                || c == 0xD || c == 0xA || c == 0x9;
    }

    public static boolean isHighXMLCharacter(int c) {
        return c >= 0x10000 && c <= 0x10FFFF;
    }

    public static char[] sanitizeXMLCharacters(char[] cbuf, int off, int len) {
        int max = off + len;
        char c;
        char lowSurrogate;
        int nextIndex;
        for (int i = off; i < max; i++) {
            c = cbuf[i];
            if (Character.isHighSurrogate(c) && (nextIndex = i + 1) < max
                    && Character.isLowSurrogate((lowSurrogate = cbuf[nextIndex]))) {
                int codePoint = Character.toCodePoint(c, lowSurrogate);
                if (!isHighXMLCharacter(codePoint)) {
                    Arrays.fill(cbuf, i, nextIndex + 1, REPLACEMENT);
                }
                i = nextIndex;
            } else if (!is16BitXMLCharacter(c)) {
                cbuf[i] = REPLACEMENT;
            }
        }
        return cbuf;
    }

    public static void writeSanitizedXMLCharacters(char[] cbuf, int off, int len, ContentHandler ch) throws SAXException {
        sanitizeXMLCharacters(cbuf, off, len);
        ch.characters(cbuf, off, len);
    }

    @Override
    public int read() throws IOException {
        int c = super.read();
        return (is16BitXMLCharacter(c) ? c : REPLACEMENT);
    }
}
