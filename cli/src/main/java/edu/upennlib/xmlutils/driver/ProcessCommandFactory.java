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
    public Command newCommand(String[] args, boolean first, boolean last) {
        return new ProcessCommand(args, first, last);
    }

    @Override
    public String getKey() {
        return "process";
    }

    private static class ProcessCommand extends MultiOutCommand {

        private File xsl;
        protected OptionSpec<File> xslSpec;
        private final String[] args;
        private TXMLFilter txf;

        public ProcessCommand(String[] args, boolean first, boolean last) {
            super(args, first, last);
            xslSpec = parser.acceptsAll(Flags.XSL_FILE_ARG, "xsl file defining processing templates").withRequiredArg().ofType(File.class);
            this.args = args;
        }

        @Override
        protected boolean init(OptionSet options) {
            boolean ret = super.init(options);
            xsl = options.valueOf(xslSpec);
            return ret;
        }
        
        @Override
        public XMLFilter getXMLFilter(File inputBase) {
            if (txf != null) {
                return txf;
            }
            if (!init(parser.parse(args))) {
                return null;
            }
            try {
                txf = new TXMLFilter(new StreamSource(xsl));
            } catch (TransformerConfigurationException ex) {
                throw new RuntimeException(ex);
            }
            if (filesFrom != null) {
                txf.setInputType(QueueSourceXMLFilter.InputType.indirect);
            }
            if (last) {
                Transformer t;
                try {
                    t = TransformerFactory.newInstance("net.sf.saxon.TransformerFactoryImpl", null).newTransformer();
                } catch (TransformerConfigurationException ex) {
                    throw new RuntimeException(ex);
                }
                txf.configureOutputTransformer(t);
                if (baseName != null) {
                    File resolvedBase;
                    if (output.isDirectory()) {
                        resolvedBase = new File(output.toURI().resolve(baseName.toURI()));
                    } else {
                        resolvedBase = baseName;
                    }
                    txf.setOutputCallback(new IncrementingFileCallback(0,
                            t, suffixLength, resolvedBase, outputExtension));
                } else if ("-".equals(output.getPath())) {
                    txf.setOutputCallback(new StdoutCallback(t));
                } else if (!output.isDirectory()) {
                    txf.setOutputCallback(new StaticFileCallback(t, output));
                } else {
                    txf.setOutputCallback(new BaseRelativeFileCallback(input, output, t, outputExtension));
                }
            }
            return txf;
        }

        @Override
        public CommandType getCommandType() {
            return CommandType.PASS_THROUGH;
        }

        @Override
        public File inputBase() {
            if (input == null) {
                return null;
            } else if ("-".equals(input.getPath())) {
                return null;
            } else if (!input.isDirectory()) {
                return null;
            } else {
                return input;
            }
        }
    }

}
