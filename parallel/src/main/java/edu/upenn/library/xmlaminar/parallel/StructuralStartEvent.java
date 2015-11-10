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

import org.xml.sax.Attributes;
import org.xml.sax.helpers.AttributesImpl;

class StructuralStartEvent {

    public static enum StructuralStartEventType { DOCUMENT, PREFIX_MAPPING, ELEMENT }

    public final StructuralStartEventType type;
    public final String one;
    public final String two;
    public final String three;
    public final Attributes atts;

    public StructuralStartEvent() {
        type = StructuralStartEventType.DOCUMENT;
        one = null;
        two = null;
        three = null;
        atts = null;
    }

    public StructuralStartEvent(String prefix, String uri) {
        type = StructuralStartEventType.PREFIX_MAPPING;
        this.one = prefix;
        this.two = uri;
        three = null;
        atts = null;
    }

    public StructuralStartEvent(String uri, String localName, String qName, Attributes atts) {
        type = StructuralStartEventType.ELEMENT;
        one = uri;
        two = localName;
        three = qName;
        this.atts = new AttributesImpl(atts);
    }

    @Override
    public String toString() {
        switch (type) {
            case DOCUMENT:
                return StructuralStartEventType.DOCUMENT.toString();
            case PREFIX_MAPPING:
                return StructuralStartEventType.PREFIX_MAPPING.toString().concat(one);
            case ELEMENT:
                StringBuilder sb = new StringBuilder();
                sb.append(StructuralStartEventType.ELEMENT).append('<').append(three).append('>');
                return buildAttsString(sb, atts).toString();
            default:
                throw new IllegalStateException();
        }
    }
    
    private static StringBuilder buildAttsString(StringBuilder sb, Attributes atts) {
        int length = atts.getLength();
        if (length < 1) {
            return sb;
        } else {
            int i = 0;
            sb.append('{');
            sb.append(atts.getQName(i)).append('=').append(atts.getValue(i));
            while (++i < length) {
                sb.append(", ");
                sb.append(atts.getQName(i)).append('=').append(atts.getValue(i));
            }
            sb.append('}');
        }
        return sb;
    }
}