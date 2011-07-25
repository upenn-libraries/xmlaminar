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

package edu.upennlib.ingestor;

import java.util.List;
import java.util.Properties;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

/**
 * Specify command line options for the Driver.  Uses commons-cli.
 * @author Peter Cline
 */
public class DriverOptions {
    private Options options;
    private Logger logger = Logger.getLogger(this.getClass());
    private Properties latestProps = null;

    /**
     * Instantiate commandline options and Options object.
     */
    public DriverOptions() {
        options = new Options();
        
        Option spring = OptionBuilder.withArgName("spring")
            .hasArg()
            .withDescription("Full path for spring xml file to use")
            .create("spring");

        Option property = OptionBuilder.withArgName("property=value")
            .hasArgs(2)
            .withValueSeparator()
            .withDescription("Use value for given property")
            .create("D");

        Option printProperties = OptionBuilder.withArgName("print-properties")
            .hasArgs(0)
            .withDescription("Print properties and exit")
            .create("p");

        Option dryrun = OptionBuilder.withArgName("dryrun")
            .hasArgs(0)
            .withDescription("Dry run of crawler action; print results and exit")
            .create("d");

        options.addOption(spring);
        options.addOption(property);
        options.addOption(printProperties);
        options.addOption(dryrun);
    }

    /**
     * Parse command line args.  Available arguments
     * <ul>
     * <li><b>-spring</b> full path for spring xml file to ue</li>
     * <li><b>-Dproperty=value</b> use value for given property.  overrides values in .properties file</li>
     * </ul>
     * @param args String[] of command line args
     * @return Properties object with arguments instantiated
     * @throws ParseException if parsing fails for some reason
     */
    public Properties parseArgs(String[] args) throws ParseException {
        Properties properties = new Properties();

        CommandLineParser parser = new GnuParser();
        CommandLine line = parser.parse(options, args);

        for (Option opt : line.getOptions()) {
            if (line.hasOption(opt.getOpt())) {
                // options with multiple args will generate a Properties object
                if (opt.hasArgs()) {
                    properties.putAll(line.getOptionProperties(opt.getOpt()));
                } else if (opt.hasArg()) {
                    // others will just have the opt name and a value
                    properties.setProperty(opt.getOpt(), opt.getValue());
                } else {
                    properties.setProperty(opt.getOpt(), "");
                }
            }
        }
        List remainingArgs = line.getArgList();

        if (remainingArgs.size() != 1) {
            throw new ParseException("One argument past options expected: properties file");
        }
        properties.setProperty("prop", (String) remainingArgs.get(0));

        latestProps = properties;

        return properties;
    }

    /**
     * Display usage message.  Generally used in case of ParseException being thrown by parseArgs
     */
    public void printHelp() {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp("process [options] propertiesFile", options);

        if (latestProps != null) {
            System.out.println();
            System.out.println("Your arguments were parsed like this:");
            latestProps.list(System.out);
        }
    }



}
