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

import edu.upennlib.paralleltransformer.JoiningXMLFilter;
import edu.upennlib.paralleltransformer.LevelSplittingXMLFilter;
import edu.upennlib.paralleltransformer.QueueSourceXMLFilter;
import edu.upennlib.paralleltransformer.TXMLFilter;
import edu.upennlib.paralleltransformer.callback.BaseRelativeFileCallback;
import edu.upennlib.paralleltransformer.callback.IncrementingFileCallback;
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
class JoinCommandFactory extends CommandFactory {
    
    static {
        registerCommandFactory(new JoinCommandFactory());
    }
    
    @Override
    public Command newCommand(String[] args, boolean first, boolean last) {
        return new JoinCommand(args, first, last);
    }

    @Override
    public String getKey() {
        return "split";
    }


}
