/*
 * MIT License
 *
 * Copyright (c) 2022 Daniel Avery
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.avery.vinyl;

import java.util.Comparator;

/**
 * Common utils
 */
class Utils {
    /**
     * Totally unchecked cast, for when a normal cast is illegal, but we know the cast is safe.
     */
    @SuppressWarnings("unchecked")
    static <T> T cast(Object o) {
        return (T) o;
    }
    
    /**
     * Natural order, nulls first/lowest.
     */
    static Comparator<Object> DEFAULT_COMPARATOR = cast((Comparator<Comparable<Object>>) (a, b) -> {
        if (a == b)
            return 0;
        if (a == null)
            return -1;
        if (b == null)
            return 1;
        return a.compareTo(b);
    });
    
    static Field<?> tempField() {
        return new Field<>("<anonymous>");
    }
}
