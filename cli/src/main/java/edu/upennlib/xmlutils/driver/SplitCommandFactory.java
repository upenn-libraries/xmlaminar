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

import edu.upennlib.paralleltransformer.LevelSplittingXMLFilter;
import edu.upennlib.paralleltransformer.QueueSourceXMLFilter;
import edu.upennlib.paralleltransformer.callback.BaseRelativeIncrementingFileCalback;
import edu.upennlib.paralleltransformer.callback.IncrementingFileCallback;
import edu.upennlib.paralleltransformer.callback.StaticFileCallback;
import edu.upennlib.paralleltransformer.callback.StdoutCallback;
import java.io.File;
import java.util.Collections;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.xml.sax.XMLFilter;

/**
 *
 * @author magibney
 */
class SplitCommandFactory extends CommandFactory {
    
    static {
        registerCommandFactory(new SplitCommandFactory());
    }
    
    @Override
    public Command newCommand(boolean first, boolean last) {
        return new SplitCommand(first, last);
    }

    @Override
    public String getKey() {
        return "split";
    }

    @Override
    public CommandFactory getConfiguringXMLFilter(boolean first, InitCommand inputBase, CommandType maxType) {
        return null;
    }

    private static class SplitCommand extends MultiOutCommand {

        private int chunkSize;
        protected OptionSpec<Integer> chunkSizeSpec;

        public SplitCommand(boolean first, boolean last) {
            super(first, last);
            chunkSizeSpec = parser.acceptsAll(Flags.SIZE_ARG, "size (in records) of output files (for split) "
                    + "or processing chunks (for process)").withRequiredArg().ofType(Integer.class)
                    .defaultsTo(100);
        }

        @Override
        protected boolean init(OptionSet options, InputCommandFactory.InputCommand inputBase) {
            boolean ret = super.init(options, inputBase);
            chunkSize = options.valueOf(chunkSizeSpec);
            return ret;
        }
        
        private static final boolean EXPECT_INPUT = true;
        
        @Override
        public XMLFilter getXMLFilter(String[] args, InputCommandFactory.InputCommand inputBase, CommandType maxType) {
            if (!init(parser.parse(args), inputBase)) {
                return null;
            }
            CommandFactory.conditionalInit(first, inputBase, EXPECT_INPUT);
            LevelSplittingXMLFilter splitter = new LevelSplittingXMLFilter(recordDepth, chunkSize);
            if (first && inputBase.filesFrom != null) {
                splitter.setInputType(QueueSourceXMLFilter.InputType.indirect);
            }
            if (false && last) {
                Transformer t;
                try {
                    t = TransformerFactory.newInstance().newTransformer();
                } catch (TransformerConfigurationException ex) {
                    throw new RuntimeException(ex);
                }
                XMLFilter outputFilter = noIndent ? null : new OutputTransformerConfigurer(Collections.singletonMap("indent", "yes"));
                if (baseName != null) {
                    File resolvedBase;
                    if (output.isDirectory()) {
                        resolvedBase = new File(output.toURI().resolve(baseName.toURI()));
                    } else {
                        resolvedBase = baseName;
                    }
                    splitter.setOutputCallback(new IncrementingFileCallback(0,
                            t, suffixLength, resolvedBase, outputExtension, outputFilter));
                } else if ("-".equals(output.getPath())) {
                    splitter.setOutputCallback(new StdoutCallback(t, outputFilter));
                } else if (!output.isDirectory()) {
                    splitter.setOutputCallback(new StaticFileCallback(t, output, outputFilter));
                } else {
                    String inBaseSystemId = inputBase.input.getSystemId();
                    File inBaseFile = inBaseSystemId == null ? null : new File(inBaseSystemId);
                    splitter.setOutputCallback(new BaseRelativeIncrementingFileCalback(inBaseFile, output, t, outputExtension, outputExtension != null, suffixLength, outputFilter));
                }
            }
            return splitter;
        }

        @Override
        public CommandType getCommandType() {
            return CommandType.SPLIT;
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
