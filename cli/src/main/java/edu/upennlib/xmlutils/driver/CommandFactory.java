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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author magibney
 */
public abstract class CommandFactory {

    private static final Map<String, CommandFactory> AVAILABLE_COMMAND_FACTORIES = 
            new HashMap<String, CommandFactory>();
    
    protected static void registerCommandFactory(CommandFactory cf) {
        AVAILABLE_COMMAND_FACTORIES.put(cf.getKey(), cf);
    }
    
    public static Map<String, CommandFactory> getAvailableCommandFactories() {
        return Collections.unmodifiableMap(AVAILABLE_COMMAND_FACTORIES);
    }
    
    public abstract Command newCommand(boolean first, boolean last);

    public abstract String getKey();

}
