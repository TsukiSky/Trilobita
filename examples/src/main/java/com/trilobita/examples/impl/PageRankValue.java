package com.trilobita.examples.impl;

import com.trilobita.commons.Computable;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;

public class PageRankValue extends Computable<Double> implements Serializable {
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
