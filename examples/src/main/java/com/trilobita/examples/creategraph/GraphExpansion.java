package com.trilobita.examples.creategraph;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GraphExpansion {

    public static void main(String[] args) {
        try {
            // Generate the graph for 3 parts
            String graph = generateGraph(50);

            // Print the graph to the console
            System.out.println(graph);

            // Save the graph to a text file
            saveGraphToFile(graph, "data/graph/Graph-451.csv");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String generateGraph(int parts) {
        // Initial edges of the graph
        List<String> edges = new ArrayList<>();
        edges.add("0,1,3");
        edges.add("0,2,10");
        edges.add("0,7,7");
        edges.add("1,3,3");
        edges.add("1,2,6");
        edges.add("2,1,1");
        edges.add("2,3,5");
        edges.add("3,4,2");
        edges.add("3,5,4");
        edges.add("4,6,10");
        edges.add("5,6,8");
        edges.add("7,5,2");
        edges.add("6,8,3");
        edges.add("8,9,1");

        List<String> newEdges = new ArrayList<>(edges);

        // Generate additional parts as needed
        for (int i = 1; i < parts; i++) {
            newEdges = generateNextPart(newEdges, (i - 1) * 9);
            edges.addAll(newEdges);
        }

        // Combine all edges into a single string
        return String.join("\n", edges);
    }

    private static List<String> generateNextPart(List<String> currentEdges, int offset) {
        List<String> newEdges = new ArrayList<>();
        for (String edge : currentEdges) {
            String[] parts = edge.split(",");
            int fromVertex = Integer.parseInt(parts[0]);
            int toVertex = Integer.parseInt(parts[1]);

            // Apply mapping only to vertices greater than or equal to the offset
            if (fromVertex >= offset && toVertex >= offset) {
                newEdges.add((fromVertex + 9) + "," + (toVertex + 9) + "," + parts[2]);
            }
        }
        return newEdges;
    }
    private static void saveGraphToFile(String graph, String filePath) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            writer.write(graph);
        }
    }
}