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

package vinyl;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * An opaque value representing an expression defined by {@link JoinAPI.On}.
 * @param <T> the type of the evaluated expression
 */
public class JoinExpr<T> {
    final int side;
    
    JoinExpr(int side) {
        this.side = side;
    }
    
    static class RecordExpr<T> extends JoinExpr<T> {
        final Field<T> field; // nullable
        final Function<? super Record, T> mapper;
        
        RecordExpr(Field<T> field, int side, Function<? super Record, T> mapper) {
            super(side);
            this.field = field;
            this.mapper = mapper;
        }
    }
    
    static class Expr<T> extends JoinExpr<T> {
        final Supplier<T> supplier;
        
        Expr(Supplier<T> supplier) {
            super(JoinAPI.NONE);
            this.supplier = supplier;
        }
    }
}
