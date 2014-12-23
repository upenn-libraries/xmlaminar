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

import edu.upennlib.paralleltransformer.SerializingXMLFilter;
import edu.upennlib.xmlutils.dbxml.BinaryMARCXMLReader;
import edu.upennlib.xmlutils.dbxml.RSXMLReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.transform.sax.SAXSource;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.XMLFilter;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author magibney
 */
public class MARCRSXMLReaderCommandFactory extends CommandFactory {
    static {
        registerCommandFactory(new MARCRSXMLReaderCommandFactory());
    }

    @Override
    public Command newCommand(boolean first, boolean last) {
        if (!first) {
            throw new IllegalArgumentException("type "+MARCRSXMLReaderCommandFactory.class+" must be first in pipeline");
        }
        return new MARCRSXMLReaderCommand(first, last);
    }

    @Override
    public String getKey() {
        return "marcdb-to-xml";
    }

    @Override
    public CommandFactory getConfiguringXMLFilter(boolean first, File inputBase, CommandType maxType) {
        return null;
    }
    
    private static class MARCRSXMLReaderCommand extends SQLXMLReaderCommand {

        private boolean initialized = false;
        private final BinaryMARCXMLReader mxr = new BinaryMARCXMLReader();
        private XMLFilter ret;
        protected String marcBinaryFieldLabel;
        private final OptionSpec<String> marcBinaryFieldLabelSpec;

        public MARCRSXMLReaderCommand(boolean first, boolean last) {
            super(first, last);
            marcBinaryFieldLabelSpec = parser.acceptsAll(Flags.MARC_FIELD_LABEL_ARG, "label of field containing chunked binary marc")
                    .withRequiredArg().ofType(String.class);
        }

        @Override
        protected boolean init(OptionSet options) {
            boolean ret = super.init(options);
            marcBinaryFieldLabel = options.valueOf(marcBinaryFieldLabelSpec);
            return ret;
        }

        @Override
        public XMLFilter getXMLFilter(String[] args, File inputBase, CommandType maxType) {
            if (initialized) {
                return ret;
            } else {
                initialized = true;
                if (!init(parser.parse(args))) {
                    return null;
                } else {
                    ret = new XMLFilterImpl(mxr);
                }
            }
            mxr.setName(name);
            mxr.setIdFieldLabels(parseIdFieldLabels(idFieldLabels));
            mxr.setOutputFieldLabels(new String[] {marcBinaryFieldLabel});
            try {
                mxr.setHost(host);
                mxr.setSid(sid);
                mxr.setUser(user);
                mxr.setPwd(password);
                mxr.setSql(sql);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            if (last) {
                SerializingXMLFilter serializer = new SerializingXMLFilter(output);
                if (noIndent) {
                    serializer.setParent(ret);
                } else {
                    serializer.setParent(new OutputTransformerConfigurer(ret, Collections.singletonMap("indent", "yes")));
                }
                ret = serializer;
            }
            return ret;
        }

    }

    private static final Pattern COMMA_SPLIT = Pattern.compile("\\s*([^,]+)\\s*(,|$)");
    
    private static String[] parseIdFieldLabels(String idFields) {
        Matcher m = COMMA_SPLIT.matcher(idFields);
        ArrayList<String> fieldList = new ArrayList<String>();
        while (m.find()) {
            fieldList.add(m.group(1));
        }
        return fieldList.toArray(new String[fieldList.size()]);
    }
}
