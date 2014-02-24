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

import joptsimple.OptionParser;

/**
 *
 * @author magibney
 */
public abstract class Command implements Runnable {
    
    public static enum Type {
        
        split("split a single xml input stream into multiple files"), 
        join("join multiple xml streams into a single stream; input files are enumerated "
                + "from the input-file argument"), 
        process("run parallel processing on input xml");
        
        public final String description;
        
        Type(String description) {
            this.description = description;
        }
    }
    
    protected abstract OptionParser configure(OptionParser parser, Driver.SpecStruct specs);
}
