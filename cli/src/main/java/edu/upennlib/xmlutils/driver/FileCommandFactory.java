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
import java.io.PrintStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLFilter;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author magibney
 */
class FileCommandFactory extends CommandFactory {
    
    private static final String KEY = "file";
    private static final SAXParserFactory spf;
    
    static {
        registerCommandFactory(new FileCommandFactory());
        spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
    }
    
    @Override
    public Command newCommand(boolean first, boolean last) {
        if (!first) {
            throw new IllegalStateException();
        }
        return new FileCommand(sb.length() == 0 ? null : sb.toString().trim());
    }

    @Override
    public String getKey() {
        return KEY;
    }

    private final StringBuilder sb = new StringBuilder();
    
    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        sb.append(ch, start, length);
        super.characters(ch, start, length);
    }
    
    @Override
    public CommandFactory getConfiguringXMLFilter(boolean first, Command inputBase, CommandType maxType) {
        sb.setLength(0);
        return this;
    }

    private static class FileCommand implements Command {

        private InputSource input;
        private XMLFilter ret;
        
        private final String path;
        
        public FileCommand(String path) {
            this.path = path;
        }
        
        @Override
        public XMLFilter getXMLFilter(String[] args, Command inputBase, CommandType maxType) {
            if (ret == null) {
                input = new InputSource(path != null ? path : args[0]);
                try {
                    ret = new XMLFilterImpl(spf.newSAXParser().getXMLReader());
                } catch (ParserConfigurationException ex) {
                    throw new RuntimeException(ex);
                } catch (SAXException ex) {
                    throw new RuntimeException(ex);
                }
            }
            return ret;
        }

        @Override
        public InputSource getInput() throws FileNotFoundException {
            return input;
        }

        @Override
        public File getInputBase() {
            return null;
        }

        @Override
        public void printHelpOn(OutputStream out) {
            PrintStream ps = new PrintStream(out);
            ps.println(KEY+" accepts single file argument");
        }

        @Override
        public CommandType getCommandType() {
            return CommandType.PASS_THROUGH;
        }
        
    }


}
