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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.util.Set;
import org.xml.sax.InputSource;
import org.xml.sax.XMLFilter;

/**
 *
 * @author magibney
 * @param <T> inputBase type for configuring input
 */
public interface Command<T extends InitCommand> {

    XMLFilter getXMLFilter(ArgFactory arf, T inputBase, CommandType maxType);
    
    InputSource getInput() throws FileNotFoundException;
    
    File getInputBase();
    
    void printHelpOn(OutputStream out);
    
    CommandType getCommandType();
    
    boolean handlesOutput();
    
    InitCommand inputHandler();
    
    public interface ArgFactory {
        String[] getArgs(Set<String> recognizedOptions);
    }
    
}
