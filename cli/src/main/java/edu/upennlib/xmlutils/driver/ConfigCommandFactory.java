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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.OutputStream;
import joptsimple.OptionSpec;
import org.xml.sax.InputSource;
import org.xml.sax.XMLFilter;

/**
 *
 * @author magibney
 */
public class ConfigCommandFactory extends CommandFactory {

    static {
        registerCommandFactory(new ConfigCommandFactory());
    }
    
    @Override
    public Command newCommand(String[] args, boolean first, boolean last) {
        return new ConfigCommand(args, first, last);
    }

    @Override
    public String getKey() {
        return "config-cmd";
    }

    private static class ConfigCommand extends MultiOutCommand {

        private boolean filter;
        private final OptionSpec filterSpec;
        
        public ConfigCommand(String[] args, boolean first, boolean last) {
            super(args, first, last);
            filterSpec = parser.acceptsAll(Flags.FILTER_ARG);
        }

        @Override
        public XMLFilter getXMLFilter(File inputBase, CommandType maxType) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public CommandType getCommandType() {
            return CommandType.PASS_THROUGH;
        }

        @Override
        public File getInputBase() {
            if (input == null) {
                return new File("");
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
