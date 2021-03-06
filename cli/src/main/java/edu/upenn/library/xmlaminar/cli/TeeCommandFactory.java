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

package edu.upenn.library.xmlaminar.cli;

import edu.upenn.library.xmlaminar.DumpingXMLFilter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Collections;
import org.xml.sax.InputSource;
import org.xml.sax.XMLFilter;

/**
 *
 * @author magibney
 */
class TeeCommandFactory extends CommandFactory {
    
    static {
        registerCommandFactory(new TeeCommandFactory());
    }
    
    @Override
    public Command newCommand(boolean first, boolean last) {
        if (first || last) {
            throw new IllegalArgumentException("Command \""+getKey()+"\" should not be first or last in chain");
        }
        return new TeeCommand();
    }

    @Override
    public String getKey() {
        return "tee";
    }

    @Override
    public CommandFactory getConfiguringXMLFilter(boolean first, InitCommand inputBase, CommandType maxType) {
        return null;
    }
    
    private class TeeCommand implements Command {

        private InitCommand inputBase;
        
        @Override
        public XMLFilter getXMLFilter(ArgFactory arf, InitCommand inputBase, CommandType maxType) {
            String[] args = arf.getArgs(Collections.EMPTY_SET);
            if (args.length != 1) {
                throw new IllegalArgumentException("Command \"" + getKey() + "\" should have only one argument: dumpfile");
            }
            this.inputBase = inputBase;
            File df = new File(args[0]);
            DumpingXMLFilter dxf = new DumpingXMLFilter();
            String path = df.getPath();
            if ("-".equals(path)) {
                dxf.setDumpStream(System.out);
            } else if ("-h".equals(path) || "--help".equals(path)) {
                return null;
            } else {
                dxf.setDumpFile(df);
            }
            return dxf;
        }

        @Override
        public InputSource getInput() throws FileNotFoundException {
            throw new UnsupportedOperationException("Not supported for command type "+getKey());
        }

        @Override
        public File getInputBase() {
            throw new UnsupportedOperationException("Not supported for command type "+getKey());
        }

        @Override
        public void printHelpOn(OutputStream out) {
            PrintStream ps = new PrintStream(out);
            ps.println("command \""+getKey()+"\" accepts one argument: outputFile ('-' for stdout); -h or --help for this help");
            ps.println("if outputFile ends in .gz extension, output will be gzipped");
        }

        @Override
        public CommandType getCommandType() {
            return CommandType.PASS_THROUGH;
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
