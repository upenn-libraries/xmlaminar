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

package edu.upennlib.paralleltransformer;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 *
 * @author Michael Gibney
 */
public class ExceptionPropogator implements Thread.UncaughtExceptionHandler {

    private final Thread target;
    private final Logger logger;
    private final Level level;
    private final Thread.UncaughtExceptionHandler next;

    public ExceptionPropogator(Thread target, Logger logger, Level level, Thread.UncaughtExceptionHandler next) {
        this.target = target;
        if (logger != null && level == null) {
            throw new IllegalArgumentException();
        }
        this.logger = logger;
        this.level = level;
        this.next = next;
    }

    public ExceptionPropogator(Thread target, Thread.UncaughtExceptionHandler next) {
        this(target, null, null, next);
    }

    public ExceptionPropogator(Thread target, Logger logger, Level level) {
        this(target, logger, level, null);
    }

    public ExceptionPropogator(Thread target) {
        this(target, null, null, null);
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        if (logger != null) {
            logger.log(level, "exception in thread "+t, e);
        } else {
            e.printStackTrace(System.err);
        }
        target.interrupt();
    }

}
