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

/**
 *
 * @author Michael Gibney
 */
public abstract class DelegatingSubdividable<E extends Enum<E>, T extends DelegatingSubdividable<E, T, P>, P extends ParentSubdividable<E, P, T>> implements Subdividable<E, T>, Runnable {

    private P parent;

    protected void setParent(P parent) {
        this.parent = parent;
    }

    @Override
    public T subdivide() {
        return parent.subdivide().getChild();
    }

    @Override
    public void setState(E state) {
        parent.setState(state);
    }

    @Override
    public boolean canSubdivide() {
        return parent.canSubdivide();
    }

    protected P getParent() {
        return parent;
    }

    protected void reset() {
        // NOOP default implementation
    }

    public abstract T newInstance();

}
