package com.baidu.hugegraph.computer.core.input;

import com.baidu.hugegraph.computer.core.graph.GraphFactory;

public interface Holder<T> {

    public abstract T convert(InputFilter inputFilter,
                              GraphFactory graphFactory);

    public abstract void clearProperties();
}
