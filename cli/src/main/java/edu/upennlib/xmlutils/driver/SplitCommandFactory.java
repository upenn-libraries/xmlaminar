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
    public Command newCommand(String[] args, boolean first, boolean last) {
        return new SplitCommand(args, first, last);
    }

    @Override
    public String getKey() {
        return "split";
    }

    private static class SplitCommand extends MultiOutCommand {

        private int chunkSize;
        protected OptionSpec<Integer> chunkSizeSpec;
        private final String[] args;

        public SplitCommand(String[] args, boolean first, boolean last) {
            super(args, first, last);
            chunkSizeSpec = parser.acceptsAll(Flags.SIZE_ARG, "size (in records) of output files (for split) "
                    + "or processing chunks (for process)").withRequiredArg().ofType(Integer.class)
                    .defaultsTo(100);
            this.args = args;
        }

        @Override
        protected boolean init(OptionSet options) {
            boolean ret = super.init(options);
            chunkSize = options.valueOf(chunkSizeSpec);
            return ret;
        }
        
        @Override
        public XMLFilter getXMLFilter(File inputBase, CommandType maxType) {
            if (!init(parser.parse(args))) {
                return null;
            }
            LevelSplittingXMLFilter splitter = new LevelSplittingXMLFilter(recordDepth, chunkSize);
            if (filesFrom != null) {
                splitter.setInputType(QueueSourceXMLFilter.InputType.indirect);
            }
            if (last) {
                Transformer t;
                try {
                    t = TransformerFactory.newInstance().newTransformer();
                } catch (TransformerConfigurationException ex) {
                    throw new RuntimeException(ex);
                }
                if (baseName != null) {
                    File resolvedBase;
                    if (output.isDirectory()) {
                        resolvedBase = new File(output.toURI().resolve(baseName.toURI()));
                    } else {
                        resolvedBase = baseName;
                    }
                    splitter.setOutputCallback(new IncrementingFileCallback(0,
                            t, suffixLength, resolvedBase, outputExtension));
                } else if ("-".equals(output.getPath())) {
                    splitter.setOutputCallback(new StdoutCallback(t));
                } else if (!output.isDirectory()) {
                    splitter.setOutputCallback(new StaticFileCallback(t, output));
                } else {
                    splitter.setOutputCallback(new BaseRelativeIncrementingFileCalback(input, output, t, outputExtension, outputExtension != null, suffixLength));
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