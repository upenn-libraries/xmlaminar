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

package edu.upennlib.ingestor.subject;

import java.io.Serializable;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author michael
 */
public class SubjectNode implements Serializable {

    private static Pattern p = Pattern.compile("^(.*?)(--(.*))?$");
    private Map<String, SubjectNode> subDivisions = null;
    private Map<String, SubjectNode> subHeadings = new HashMap<String, SubjectNode>(2);
    private HashSet<String> prefLabels = new HashSet<String>(2);

    private String normalizeString(String input) {
        return Normalizer.normalize(input, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "");
    }

    public Set<String> put(String altLabel, String prefLabel) {
        altLabel = normalizeString(altLabel);
        prefLabel = normalizeString(prefLabel);
        if (altLabel.startsWith("--")) {
            return putInternal(altLabel.substring(2), prefLabel, true);
        } else {
            return putInternal(altLabel, prefLabel, false);
        }
    }

    public SubjectNode() {
        subHeadings = new HashMap<String, SubjectNode>(418000);
        subDivisions = new HashMap<String, SubjectNode>(1280);
        prefLabels.add("");
    }

    private SubjectNode(Map<String, SubjectNode> subDivisions) {
        this.subDivisions = subDivisions;
    }

    private Set<String> putInternal(String altLabelPart, String prefLabelFull, boolean subDivision) {
        Map<String, SubjectNode> headingsMap = null;
        if (subDivision) {
            headingsMap = subDivisions;
        } else {
            headingsMap = subHeadings;
        }
        if (altLabelPart == null) {
            prefLabels.add(prefLabelFull);
            return prefLabels;
        } else {
            Matcher m = p.matcher(altLabelPart);
            if (m.find()) {
                String nextSubHeading = m.group(1);
                String followingSubHeadings = m.group(3);
                SubjectNode next = headingsMap.get(nextSubHeading);
                if (next == null) {
                    next = new SubjectNode(subDivisions);
                    headingsMap.put(nextSubHeading, next);
                }
                return next.putInternal(followingSubHeadings, prefLabelFull, false);
            } else {
                throw new RuntimeException();
            }
        }
    }

    public String resolve(String rawHeading) {
        rawHeading = normalizeString(rawHeading);
        String[] headingArray = rawHeading.split("--");
        try {
            return resolveInternal(headingArray, headingArray);
        } catch (MappingNotFoundException ex) {
            System.err.println(ex.getMessage());
            return rawHeading;
        }
    }

    private String cat(String[] sa) {
        return cat(sa, 0, sa.length);
    }

    public void printBaseSize() {
        System.out.println("subHeadings:"+subHeadings.size()+" subDivisions:"+subDivisions.size());
    }

    private String cat(String[] sa, int startIndex, int endIndex) {
        if (sa.length == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder(sa[startIndex]);
        for (int i = startIndex+1; i<endIndex; i++) {
            sb.append("--").append(sa[i]);
        }
        return sb.toString();
    }

    private String[] getSubHeadings(String[] orig) {
        if (orig.length <= 1) {
            return new String[0];
        } else {
            return Arrays.copyOfRange(orig, 1, orig.length);
        }
    }

    private String getNodeValue(String defaultValue) {
        switch (prefLabels.size()) {
            case 1:
                return prefLabels.iterator().next();
            case 0:
                return defaultValue;
            default:
                System.out.println("multiple mappings exist for \"" + defaultValue + "\": " + prefLabels.toString());
                return defaultValue;
        }

    }

    private String resolveInternal(String[] rawHeadingPart, String[] rawHeadingFull) throws MappingNotFoundException {
        if (rawHeadingPart == null || rawHeadingPart.length == 0) {
            return getNodeValue(cat(rawHeadingFull));
        } else {
            if (subHeadings.containsKey(rawHeadingPart[0])) {
                String[] followingSubHeadings = getSubHeadings(rawHeadingPart);
                return subHeadings.get(rawHeadingPart[0]).resolveInternal(followingSubHeadings, rawHeadingFull);
            } else {
                String[] followingSubHeadings = rawHeadingPart;
                StringBuilder sb = new StringBuilder(getNodeValue(cat(rawHeadingFull, 0, rawHeadingFull.length - rawHeadingPart.length)));
                for (int i = 0; i < rawHeadingPart.length; i++) {
                    if (sb.length() > 0) {
                        sb.append("--");
                    }
                    if (subDivisions.containsKey(rawHeadingPart[i])) {
                        return sb.append(subDivisions.get(rawHeadingPart[i]).resolveInternal(getSubHeadings(followingSubHeadings), followingSubHeadings)).toString();
                    } else {
                        sb.append(rawHeadingPart[i]);
                    }
                    followingSubHeadings = getSubHeadings(followingSubHeadings);
                }
                return sb.toString();
            }
        }
    }

    private final String INDENTATION = "  ";

    public String toStringVerbose() {
        StringBuilder sb = new StringBuilder();
        toStringVerbose(sb, "", false); // XXX false for main headings, true for subDivisions
        return sb.toString();
    }

    public void toStringVerbose(StringBuilder sb, String indentation, boolean subDivisions) {
        Map<String, SubjectNode> headingsMap = null;
        if (subDivisions) {
            headingsMap = this.subDivisions;
        } else {
            headingsMap = subHeadings;
        }
        if (headingsMap.isEmpty()) {
            sb.append(" = ").append(prefLabels);
        } else {
            for (Entry<String, SubjectNode> e : headingsMap.entrySet()) {
                sb.append("\n").append(indentation).append(e.getKey());
                e.getValue().toStringVerbose(sb, indentation+INDENTATION, false);
            }
        }
    }

    private class MappingNotFoundException extends Exception {

        public MappingNotFoundException(Throwable cause) {
            super(cause);
        }

        public MappingNotFoundException(String message, Throwable cause) {
            super(message, cause);
        }

        public MappingNotFoundException(String message) {
            super(message);
        }

        public MappingNotFoundException() {
        }

    }

}
