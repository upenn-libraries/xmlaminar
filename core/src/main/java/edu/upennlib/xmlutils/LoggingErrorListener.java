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

package edu.upennlib.xmlutils;

import java.util.EnumMap;
import javax.xml.transform.ErrorListener;
import javax.xml.transform.TransformerException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 *
 * @author michael
 */
public class LoggingErrorListener implements ErrorListener {

    private final Logger logger;
    public static enum ErrorLevel { WARNING, ERROR, FATAL_ERROR };
    public final EnumMap<ErrorLevel, Level> levelMap;

    public LoggingErrorListener(Logger logger, EnumMap<ErrorLevel, Level> levelMapping) {
        this.logger = logger;
        this.levelMap = levelMapping;
    }

    @Override
    public void warning(TransformerException exception) throws TransformerException {
        Level logLevel = levelMap.get(ErrorLevel.WARNING);
        if (logLevel != null && logLevel != Level.OFF && logger.isEnabledFor(logLevel)) {
            logger.log(logLevel, exception.getMessageAndLocation());
        }
    }

    @Override
    public void error(TransformerException exception) throws TransformerException {
        Level logLevel = levelMap.get(ErrorLevel.ERROR);
        if (logLevel != null && logLevel != Level.OFF && logger.isEnabledFor(logLevel)) {
            logger.log(logLevel, exception.getMessageAndLocation());
        }
    }

    @Override
    public void fatalError(TransformerException exception) throws TransformerException {
        throw exception;
    }
}
