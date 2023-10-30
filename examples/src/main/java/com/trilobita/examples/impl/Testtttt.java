package com.trilobita.examples.impl;

// Java program to demonstrate
// that if superclass is
// serializable then subclass
// is automatically serializable

import org.springframework.util.SerializationUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

// superclass A
// implementing Serializable interface

class Vertex implements Serializable {

}

class PRV extends Vertex {

}

// subclass B
// B class doesn't implement Serializable
// interface.
class VertexGroup implements Serializable {
    List<Vertex> list;
    VertexGroup(List<Vertex> l){
        this.list = l;
    }
}

class Graph extends VertexGroup {
    Graph(List<Vertex> l){
        super(l);
    }
}

class Messagee implements Serializable {
    Object content;
}

class Computable<T>{
    T value;
}

class PRValue extends Computable<Double> {

}

class Maaail implements Serializable{
    Messagee messagee;
}
// Driver class
public class Testtttt {
    public static void main(String[] args)
            throws Exception {
//        List<Vertex> vertices = new ArrayList<>();
//        vertices.add(new PRV());
//        vertices.add(new PRV());
//        vertices.add(new PRV());
//        Graph g = new Graph(vertices);
        ConcurrentHashMap<Integer, Computable> map = new ConcurrentHashMap<>();
        Computable c = new PRValue();
        c.value = 0.15;
        map.put(1, c);
        Maaail mail = new Maaail();
        Messagee messagee = new Messagee();
        messagee.content = map;
        mail.messagee = messagee;

        /* Serializing B's(subclass) object */

        //Saving of object in a file
        byte[] data = SerializationUtils.serialize(mail);
        Maaail m = (Maaail) SerializationUtils.deserialize(data);
        ConcurrentHashMap<Integer, Computable> map1 = (ConcurrentHashMap<Integer, Computable>) m.messagee.content;
        System.out.println(map1.get(1).value);
    }
}
