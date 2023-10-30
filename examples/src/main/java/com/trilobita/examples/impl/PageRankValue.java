package com.trilobita.examples.impl;

import com.trilobita.commons.Computable;

import java.io.Serializable;

public class PageRankValue implements Computable<Double>, Serializable {
    private Double value;
    public PageRankValue(Double value) {
        this.value = value;
    }

    @Override
    public Computable<Double> add(Computable<Double> computable) {
        this.value += computable.getValue();
        return this;
    }

    @Override
    public Computable<Double> minus(Computable<Double> computable) {
        this.value -= computable.getValue();
        return this;
    }

    @Override
    public Computable<Double> multiply(Computable<Double> computable) {
        this.value *= computable.getValue();
        return this;
    }

    public Computable<Double> multiply(Double num) {
        this.value *= num;
        return this;
    }


    @Override
    public Computable<Double> divide(Computable<Double> computable) {
        this.value /= computable.getValue();
        return this;
    }

    @Override
    public Double getValue() {
        return this.value;
    }

    @Override
    public void setValue(Double value) {
        this.value = value;
    }
}
