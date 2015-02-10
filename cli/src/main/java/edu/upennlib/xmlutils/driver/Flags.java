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

package edu.upennlib.xmlutils.driver;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import java.util.List;

/**
 *
 * @author magibney
 */
public class Flags {
    /*
    GENERIC OPTIONS
    */
    public static final List<String> VERBOSE_ARG = unmodifiableList(asList("v", "verbose"));
    public static final List<String> HELP_ARG = unmodifiableList(asList("h", "help"));
    
    /*
    INPUT OPTIONS
    */
    public static final List<String> INPUT_FILE_ARG = unmodifiableList(asList("i", "input"));
    public static final List<String> FILES_FROM_ARG = unmodifiableList(asList("files-from"));
    public static final List<String> FROM0_ARG = unmodifiableList(asList("0", "from0"));
    public static final List<String> INPUT_DELIMITER_ARG = unmodifiableList(asList("d", "input-delimiter"));
    
    /*
    OUTPUT OPTIONS
    */
    public static final List<String> OUTPUT_FILE_ARG = unmodifiableList(asList("o", "output"));
    public static final List<String> SUFFIX_LENGTH_ARG = unmodifiableList(asList("l", "suffix-length"));
    public static final List<String> OUTPUT_EXTENSION_ARG = unmodifiableList(asList("output-extension"));
    public static final List<String> GZIP_OUTPUT_ARG = unmodifiableList(asList("z", "gzip"));

    /*
    SPECIAL OPTIONS
    */
    public static final List<String> DEPTH_ARG = unmodifiableList(asList("r", "split-depth"));
    public static final List<String> SIZE_ARG = unmodifiableList(asList("n", "chunk-size"));
    public static final List<String> XSL_FILE_ARG = unmodifiableList(asList("x", "xsl"));
    public static final List<String> RECORD_ID_XPATH_ARG = unmodifiableList(asList("record-xpath"));
    public static final List<String> SUBDIVIDE_ARG = unmodifiableList(asList("s", "subdivide-on-failure"));
    public static final List<String> OUTPUT_BASE_NAME_ARG = unmodifiableList(asList("b", "output-basename"));
    public static final List<String> JOIN_ALL_ARG = unmodifiableList(asList("a", "all"));
    public static final List<String> NO_INDENT_ARG = unmodifiableList(asList("no-indent"));

    /*
    SQL OPTIONS
     */
    public static final List<String> NAME_ARG = unmodifiableList(asList("name"));
    public static final List<String> CONNECTION_CONFIG_FILE_ARG = unmodifiableList(asList("connection-config-file"));
    public static final List<String> ID_FIELD_LABELS_ARG = unmodifiableList(asList("id-field-labels"));
    public static final List<String> MARC_FIELD_LABEL_ARG = unmodifiableList(asList("marc-binary-field-label"));
    public static final List<String> SQL_ARG = unmodifiableList(asList("sql"));
    public static final List<String> LOOKAHEAD_FACTOR_ARG = unmodifiableList(asList("l", "lookahead"));

}
