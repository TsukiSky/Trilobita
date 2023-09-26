package com.trilobita.core.graph.vertex;


public interface Combiner {
    /**
     * <p>
     *     Combine the messages into one message
     * </p>
     * @param messages messages to combine
     * @return the combined message
     */
    Object combine(Iterable<?> messages);
}
