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

import edu.upennlib.configurationutils.BPP;
import edu.upennlib.configurationutils.ConfigUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.springframework.core.io.ClassPathResource;

/**
 * Main driver for digital object processing.  General point of entry is main(String[] args)
 * @author Peter Cline
 */
public final class Driver {

    private Driver() { }

    private static Logger logger = Logger.getLogger(Driver.class);

    /**
     * Aggregate all the properties necessary for a run.  These include:
     * <ul>
     * <li>command line properties</li>
     * <li>collection properties (from file specified on command line)</li>
     * <li>default properties (from default.properties within the jar)</li>
     * </ul>
     * Order of importance:
     * commandLineProps > collectionProps > defaultProps
     * @param args command line args
     * @return Properties object combining all of these
     */
    public static Properties getCombinedProps(String [] args) {
        DriverOptions options = new DriverOptions();

        /*
         * command line properties
         */
        Properties cmdLineProps = null;
        try {
            System.out.println("parsing: "+Arrays.asList(args));
            cmdLineProps = options.parseArgs(args);
        } catch (ParseException ex) {
            logger.fatal("Failed to parse command line arguments.  Check your syntax!  Exiting.");
            options.printHelp();
            throw new RuntimeException("Could not parse arguments.", ex);
        }

        /*
         * properties specified by prop file from command line
         */
        Properties collectionProps = new Properties();
        String collectionPropertyFilePath = cmdLineProps.getProperty("prop");
        if (collectionPropertyFilePath != null) {
            try {
                collectionProps.load(new FileInputStream(new File(cmdLineProps.getProperty("prop"))));
            } catch (IOException ex) {
                logger.fatal("Could not load properties file specified on command line: "
                        + cmdLineProps.getProperty("prop"));
                throw new RuntimeException("Could not open collection properties file.");
            }
        }

        /*
         * default properties
         */
        Properties defaultProps = new Properties();
        try {
            defaultProps.load((new ClassPathResource("defaults.properties")).getInputStream());
        } catch (IOException ex) {
            logger.fatal("Could not load default properties file. Exception thrown: "+ex);
            throw new RuntimeException("Could not open default properties file.");
        }

        /*
         * Order of importance:
         * commandLineProps > collectionProps > defaultProps
         */

        Properties combinedProps = new Properties();
        
        combinedProps.putAll(defaultProps);
        combinedProps.putAll(collectionProps);
        combinedProps.putAll(cmdLineProps);

        //HashMap<String, HashMap<String,TreeMap<Integer,HashMap<String,String>>>> indexedProps = ConfigUtils.extractIndexedProps(combinedProps);
        
        if (cmdLineProps.containsKey("p")) {
            LinkedHashMap<String, Properties> orderedProperties = new LinkedHashMap<String, Properties>();
            orderedProperties.put("cmdLine", cmdLineProps);
            orderedProperties.put("collection", collectionProps);
            orderedProperties.put("default", defaultProps);
            ConfigUtils.printOverriddenProperties(orderedProperties);
            System.out.println("\nCombined Properties:");
            ConfigUtils.printSortedProperties(combinedProps);
            System.out.println("\nCollapsed Properties:");
            HashMap<String, ArrayList<HashMap<String, ArrayList<String>>>> indexedProps = ConfigUtils.extractIndexedProps(combinedProps, null);
            System.out.println(ConfigUtils.expandCollapsedProps(indexedProps));
            return null;
        }


        createTempPropertiesFile(combinedProps);

        return combinedProps;
    }

    /**
     * Take the properties object supplied, write a new temporary properties file containg the
     * contents.  Also, set a "tempPropFile" property within it.  Useful if you need to pass
     * a String for property file location, instead of a Properties object
     * @param properties Props to be written
     * @return File location of temporary Properties file
     */
    public static File createTempPropertiesFile(Properties properties) {
        File file = null;
        try {
            file = File.createTempFile("ingestor.", ".properties");
            properties.put("tempPropFile", file.getAbsolutePath());
            properties.store(new FileOutputStream(file), 
                    "Temporary prop store; need a genuine properties File for some methods.");
            file.deleteOnExit();
        } catch (IOException ex) {
            ex.printStackTrace();
            return null;
        }
        return file;
    }

    private static void logBeanDefinitionStoreException(String exceptionMessage) {
        String logMessage = "";
        Matcher matches = Pattern.compile("Could not resolve placeholder (.*)").matcher(exceptionMessage);
        if (matches.find() && matches.groupCount() == 1) {
            logMessage = "Missing parameter: ";
            logMessage += matches.group(1);
            logMessage += ". Please add to .properties file or specify at command line with -D option.";
        } else {
            // if it's a different kind of message, just print it out
            logMessage = exceptionMessage;
        }
        logger.fatal(logMessage);
        System.out.println(logMessage);
    }

    /**
     * Run the digital object processor.  
     * @param args the command line arguments
     * @throws Exception from FileServer starting/stopping
     */
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            args = new String[1];
            args[0] = "franklin.properties";
        }

        Properties properties = getCombinedProps(args);

        if (properties == null) {
            return;
        }

        logger.trace("Setting up appenders.");
        Logger.getRootLogger().removeAllAppenders();
        PropertyConfigurator.configure(properties);

        logger.info("Beginning processing of "+(args.length > 0 ? args[args.length-1] : "[no-collection-propfile-set]")+".");

        boolean springFromCommandLine = (properties.getProperty("spring") != null);
        String spring = springFromCommandLine ?
            properties.getProperty("spring") : "spring-main.xml";
        
        logger.trace("Creating application context.");
        ConfigurableApplicationContext context = null;
        try {
            context = getNewContext(properties, spring, springFromCommandLine);
        } catch (BeanDefinitionStoreException ex) {
            logBeanDefinitionStoreException(ex.getMessage());
            throw new RuntimeException("Exiting due to above Spring initialization error.");
        }
        HashMap<String, String> instanceNameToBeanName = new HashMap<String, String>();
        HashMap<String, ArrayList<HashMap<String, ArrayList<String>>>> collapsedProps = ConfigUtils.extractIndexedProps(properties, instanceNameToBeanName);
        BPP bpp = (BPP) context.getBean("bpp");
        bpp.setDefaultProperties(properties);
        bpp.setPrototypeBeanPropOverrides(collapsedProps);
        bpp.setInstanceNameToBeanName(instanceNameToBeanName);

        SAXIngestor ingestor = (SAXIngestor) context.getBean("ingestor");
        long start = System.currentTimeMillis();
        ingestor.run();
        System.out.println("SAX ingestor duration: "+(System.currentTimeMillis() - start));
    }

    public static ConfigurableApplicationContext getNewContext(Properties prop, String spring, boolean xmlFromFileSystem) {
        ConfigurableApplicationContext context;
        if (xmlFromFileSystem) {
            context = new FileSystemXmlApplicationContext(spring);
        } else {
            context = new ClassPathXmlApplicationContext(spring);
        }
        return context;
    }

}
