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

package edu.upenn.library.xmlaminar.parallel;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 * @author magibney
 */
class ThrottlingExecutorService implements ExecutorService {
    private static final int DEFAULT_MAX_THREADS = Runtime.getRuntime().availableProcessors();
    private final ExecutorService backing;
    private final ExecutorCompletionService service;
    private final Semaphore permits;
    private final BlockingQueue completionQueue;

    public ThrottlingExecutorService(ExecutorService backing, int maxThreads) {
        this.backing = backing;
        this.permits = new Semaphore(maxThreads);
        this.completionQueue = new PermitReleaser(permits);
        this.service = new ExecutorCompletionService(backing, completionQueue);
    }
    
    public ThrottlingExecutorService(ExecutorService backing) {
        this(backing, DEFAULT_MAX_THREADS);
    }

    private static class PermitReleaser<T extends Future<?>> extends BlockingQueueImpl<T> {
        
        private final Semaphore permits;
        
        private PermitReleaser(Semaphore permits) {
            this.permits = permits;
        }

        @Override
        public boolean add(T e) {
            permits.release();
            return true;
        }
        
    }
    
    @Override
    public void shutdown() {
        backing.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return backing.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return backing.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return backing.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return backing.awaitTermination(timeout, unit);
    }

    private void acquirePermit() {
        try {
            permits.acquire();
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    @Override
    public <T> Future<T> submit(Callable<T> task) {
        acquirePermit();
        try {
            return service.submit(task);
        } catch (RuntimeException ex) {
            permits.release();
            throw ex;
        }
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        acquirePermit();
        try {
            return service.submit(task, result);
        } catch (RuntimeException ex) {
            permits.release();
            throw ex;
        }
    }

    @Override
    public Future<?> submit(Runnable task) {
        acquirePermit();
        try {
            return service.submit(task, null);
        } catch (RuntimeException ex) {
            permits.release();
            throw ex;
        }
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void execute(Runnable command) {
        service.submit(command, null);
    }
    
}
