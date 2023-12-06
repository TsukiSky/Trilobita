package com.trilobita.examples.creategraph;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GraphExpansionWidth {

    public static void main(String[] args) {
        try {
            // Generate the graph for 3 parts
            String graph = generateGraph(10);

            // Print the graph to the console
            System.out.println(graph);

            // Save the graph to a text file
            saveGraphToFile(graph, "data/graph/GraphWidth-91.csv");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String generateGraph(int parts) {
        // Original edges of the graph
        List<String> originalEdges = new ArrayList<>();
        originalEdges.add("0,1,3");
        originalEdges.add("0,2,10");
        originalEdges.add("0,7,7");
        originalEdges.add("1,3,3");
        originalEdges.add("1,2,6");
        originalEdges.add("2,1,1");
        originalEdges.add("2,3,5");
        originalEdges.add("3,4,2");
        originalEdges.add("3,5,4");
        originalEdges.add("4,6,10");
        originalEdges.add("5,6,8");
        originalEdges.add("7,5,2");
        originalEdges.add("6,8,3");
        originalEdges.add("8,9,1");

        // All edges, starting with the original
        List<String> allEdges = new ArrayList<>(originalEdges);

        // Generate additional parts as needed
        for (int i = 1; i < parts; i++) {
            int increment = 9 * i;
            List<String> newEdges = generateNextPart(originalEdges, increment);
            allEdges.addAll(newEdges);
        }

        // Combine all edges into a single string
        return String.join("\n", allEdges);
    }

    private static List<String> generateNextPart(List<String> originalEdges, int increment) {
        List<String> newEdges = new ArrayList<>();
        for (String edge : originalEdges) {
            String[] parts = edge.split(",");
            int fromVertex = Integer.parseInt(parts[0]);
            int toVertex = Integer.parseInt(parts[1]);
            if (fromVertex == 0){
                newEdges.add((fromVertex ) + "," + (toVertex + increment) + "," + parts[2]);
            }
            else {
                newEdges.add((fromVertex + increment) + "," + (toVertex + increment) + "," + parts[2]);
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