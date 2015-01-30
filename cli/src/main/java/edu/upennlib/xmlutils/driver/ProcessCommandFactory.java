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

import edu.upennlib.paralleltransformer.QueueSourceXMLFilter;
import edu.upennlib.paralleltransformer.TXMLFilter;
import edu.upennlib.paralleltransformer.callback.BaseRelativeFileCallback;
import edu.upennlib.paralleltransformer.callback.BaseRelativeIncrementingFileCalback;
import edu.upennlib.paralleltransformer.callback.IncrementingFileCallback;
import edu.upennlib.paralleltransformer.callback.StaticFileCallback;
import edu.upennlib.paralleltransformer.callback.StdoutCallback;
import java.io.File;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamSource;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.xml.sax.XMLFilter;

/**
 *
 * @author magibney
 */
class ProcessCommandFactory extends CommandFactory {
    
    static {
        registerCommandFactory(new ProcessCommandFactory());
    }
    
    @Override
    public Command newCommand(boolean first, boolean last) {
        return new ProcessCommand(first, last);
    }

    @Override
    public String getKey() {
        return "process";
    }

    @Override
    public CommandFactory getConfiguringXMLFilter(boolean first, InitCommand inputBase, CommandType maxType) {
        return null;
    }

    private static class ProcessCommand extends MultiOutCommand {

        private File xsl;
        protected OptionSpec<File> xslSpec;
        private String recordIdXPath;
        protected OptionSpec<String> recordIdXPathSpec;
        private boolean subdivide;
        protected OptionSpec subdivideSpec;
        private TXMLFilter txf;

        public ProcessCommand(boolean first, boolean last) {
            super(first, last);
            xslSpec = parser.acceptsAll(Flags.XSL_FILE_ARG, "xsl file defining processing templates").withRequiredArg().ofType(File.class);
            recordIdXPathSpec = parser.acceptsAll(Flags.RECORD_ID_XPATH_ARG, "xpath specifying record id location").withRequiredArg().ofType(String.class);
            subdivideSpec = parser.acceptsAll(Flags.SUBDIVIDE_ARG, "define behavior on processing failure");
        }

        @Override
        protected boolean init(OptionSet options, InputCommandFactory.InputCommand inputBase) {
            boolean ret = super.init(options, inputBase);
            xsl = options.valueOf(xslSpec);
            recordIdXPath = options.valueOf(recordIdXPathSpec);
            subdivide = options.has(subdivideSpec);
            return ret;
        }
        
        private static final boolean EXPECT_INPUT = true;
        
        @Override
        public XMLFilter getXMLFilter(String[] args, InputCommandFactory.InputCommand inputBase, CommandType maxType) {
            if (txf != null) {
                return txf;
            }
            if (!init(parser.parse(args), inputBase)) {
                return null;
            }
            CommandFactory.conditionalInit(first, inputBase, EXPECT_INPUT);
            try {
                txf = new TXMLFilter(new StreamSource(xsl), recordIdXPath, subdivide);
            } catch (TransformerConfigurationException ex) {
                throw new RuntimeException(ex);
            }
            if (first && inputBase.filesFrom != null) {
                txf.setInputType(QueueSourceXMLFilter.InputType.indirect);
            }
            if (false && last) {
                if (true) {
                    throw new AssertionError("XXX");
                }
                Transformer t;
                try {
                    t = TransformerFactory.newInstance("net.sf.saxon.TransformerFactoryImpl", null).newTransformer();
                } catch (TransformerConfigurationException ex) {
                    throw new RuntimeException(ex);
                }
                // No output configuring filter here; should be handled directly in stylesheet
                if (baseName != null) {
                    File resolvedBase;
                    if (output.isDirectory()) {
                        resolvedBase = new File(output.toURI().resolve(baseName.toURI()));
                    } else {
                        resolvedBase = baseName;
                    }
                    txf.setOutputCallback(new IncrementingFileCallback(0,
                            t, suffixLength, resolvedBase, outputExtension, null));
                } else if ("-".equals(output.getPath())) {
                    txf.setOutputCallback(new StdoutCallback(t));
                } else if (!output.isDirectory()) {
                    txf.setOutputCallback(new StaticFileCallback(t, output));
                } else if (maxType == CommandType.SPLIT) {
                    txf.setOutputCallback(new BaseRelativeIncrementingFileCalback(inputBase.getInputBase(), output, t, outputExtension, outputExtension != null, suffixLength, null));
                } else {
                    txf.setOutputCallback(new BaseRelativeFileCallback(inputBase.getInputBase(), output, t, outputExtension, outputExtension != null));
                }
            }
            return txf;
        }

        @Override
        public CommandType getCommandType() {
            return CommandType.PASS_THROUGH;
        }

        @Override
        public File getInputBase() {
            return inputBase.getInputBase();
        }

        @Override
        public boolean handlesOutput() {
            return false;
        }

        @Override
        public InitCommand inputHandler() {
            return inputBase;
        }
    }

}
