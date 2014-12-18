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
import org.slf4j.Logger;

/**
 *
 * @author michael
 */
public class LoggingErrorListener implements ErrorListener {

    private final Logger logger;
    public static enum ErrorLevel { WARNING, ERROR, FATAL_ERROR };
    public static enum LogLevel { OFF, TRACE, DEBUG, INFO, WARN, ERROR };
    public final EnumMap<ErrorLevel, LogLevel> levelMap;
    private static final EnumMap<ErrorLevel, LogLevel> DEFAULT_LEVEL_MAP;
    
    static {
        DEFAULT_LEVEL_MAP = new EnumMap<ErrorLevel, LogLevel>(ErrorLevel.class);
        DEFAULT_LEVEL_MAP.put(ErrorLevel.WARNING, LogLevel.INFO);
        DEFAULT_LEVEL_MAP.put(ErrorLevel.ERROR, LogLevel.WARN);
        DEFAULT_LEVEL_MAP.put(ErrorLevel.FATAL_ERROR, LogLevel.ERROR);
    }
    
    public LoggingErrorListener(Logger logger) {
        this(logger, new EnumMap<ErrorLevel, LogLevel>(DEFAULT_LEVEL_MAP));
    }
    
    public LoggingErrorListener(Logger logger, EnumMap<ErrorLevel, LogLevel> levelMapping) {
        this.logger = logger;
        this.levelMap = levelMapping;
    }

    @Override
    public void warning(TransformerException exception) throws TransformerException {
        log(exception, ErrorLevel.WARNING);
    }

    @Override
    public void error(TransformerException exception) throws TransformerException {
        log(exception, ErrorLevel.ERROR);
    }
    
    private void log(TransformerException ex, ErrorLevel el) {
        LogLevel logLevel = levelMap.get(el);
        switch (logLevel) {
            case TRACE:
                if (logger.isTraceEnabled()) {
                    logger.trace(ex.getMessageAndLocation());
                }
                break;
            case DEBUG:
                if (logger.isDebugEnabled()) {
                    logger.debug(ex.getMessageAndLocation());
                }
                break;
            case INFO:
                if (logger.isInfoEnabled()) {
                    logger.info(ex.getMessageAndLocation());
                }
                break;
            case WARN:
                if (logger.isWarnEnabled()) {
                    logger.warn(ex.getMessageAndLocation());
                }
                break;
            case ERROR:
                if (logger.isErrorEnabled()) {
                    logger.error(ex.getMessageAndLocation());
                }
                break;
        }
                
    }

    @Override
    public void fatalError(TransformerException exception) throws TransformerException {
        log(exception, ErrorLevel.FATAL_ERROR);
        throw exception;
    }
}
