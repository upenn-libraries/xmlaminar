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

package edu.upennlib.xmlutils.dbxml;

import edu.upennlib.dbutils.ConnectionException;
import edu.upennlib.subjectremediation.SubjectTrieLoader;
import edu.upennlib.subjectremediation.SubjectTrieTraverser;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.CharBuffer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

/**
 *
 * @author michael
 */
public class SubjectMARCFieldModifier implements MARCFieldModifier {

    private static final Logger logger = Logger.getLogger(SubjectMARCFieldModifier.class);
    private final int[] resolvedLength = new int[1];
    private SubjectTrieTraverser lookup;

    private static final PrintStream rawOut;
    private static final PrintStream modOut;

    static {
        try {
            rawOut = new PrintStream("/tmp/raw_headings.txt");
            modOut = new PrintStream("/tmp/mod_headings.txt");
        } catch (FileNotFoundException ex) {
            throw new RuntimeException(ex);
        }
    }

    public SubjectTrieTraverser getSubjectRemediationTable() {
        return lookup;
    }

    public void setSubjectRemediationTable(SubjectTrieTraverser snt) {
        lookup = snt;
    }

    private static final int PREFIX_LENGTH = 4;

    @Override
    public char[] modifyField(String tag, CharBuffer original, char[] out, int[] startAndEnd) {
        // TODO separate handling of different heading types (1 for children, etc.)?
        if (tag.charAt(0) == '6' && original.charAt(1) == '0') {
            char[] origArray = original.array();
            int offset = original.arrayOffset();
            int endIndex = offset + original.length() - 1;
            boolean foundEndIndex = false;
            while (!foundEndIndex) {
                switch (origArray[endIndex]) {
                    case '.':
                    case BinaryMARCXMLReader.FT:
                        endIndex--;
                        break;
                    default:
                        endIndex++;
                        foundEndIndex = true;
                }
            }
            CharSequence value = original.subSequence(PREFIX_LENGTH, endIndex - offset);
            int remaining = offset + original.length() - endIndex;
            char[] modifiedOut = lookup.resolveStrict(value, out, PREFIX_LENGTH, remaining, resolvedLength);
            if (modifiedOut != null) {
                if (logger.isDebugEnabled()) {
                    String replacement = new String(modifiedOut, PREFIX_LENGTH, resolvedLength[0]);
                    logger.debug("resolved "+value+" to "+replacement);
                }
                rawOut.println(value);
                modOut.println(new String(modifiedOut, PREFIX_LENGTH, resolvedLength[0]));
                out = modifiedOut;
                System.arraycopy(origArray, offset + original.position(), out, 0, PREFIX_LENGTH);
                int position = PREFIX_LENGTH + resolvedLength[0];
                System.arraycopy(origArray, endIndex, out, position, remaining);
                position += remaining;
                startAndEnd[0] = 0;
                startAndEnd[1] = position;
                return out;
            }
        }
        return null;
    }

    public static void main(String[] args) throws ConnectionException, TransformerConfigurationException, TransformerException, IOException {
        BasicConfigurator.configure();
        SubjectTrieLoader stl = new SubjectTrieLoader();
        SubjectTrieTraverser lookup = stl.load();
        SubjectMARCFieldModifier modifier = new SubjectMARCFieldModifier();
        modifier.setSubjectRemediationTable(lookup);
        BinaryMARCXMLReader bmxReader = BinaryMARCXMLReader.getTestInstance(3000000, 3100000);
        bmxReader.setFieldModifier(modifier);
        BinaryMARCXMLReader.runTestInstanceToFile(bmxReader);
        modOut.close();
        rawOut.close();
    }

}
