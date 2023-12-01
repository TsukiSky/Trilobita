package com.trilobita.examples.pagerank;

import com.trilobita.core.graph.Graph;
import com.trilobita.examples.pagerank.vertex.PageRankVertex;
import com.trilobita.runtime.parser.JsonParser;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public class PageRankParserDemo {
    public static void main(String[] args) throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        Graph<Double> graph = JsonParser.read("data/graph/PageRankGraph.json", PageRankVertex.class);

        JsonParser.store("data/graph", "TTTTT.json", graph);
    }
}
