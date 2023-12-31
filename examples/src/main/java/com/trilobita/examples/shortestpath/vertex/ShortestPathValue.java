package com.trilobita.examples.shortestpath.vertex;

import com.trilobita.core.common.Computable;
import lombok.Data;

@Data
public class ShortestPathValue implements Computable<Double> {
    private Double value;

    public ShortestPathValue(double value) {
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

    @Override
    public int compareTo(Double other) {
        return this.value.compareTo(other);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Computable<Double> clone() {
        try {
            return (Computable<Double>) super.clone();
        } catch (CloneNotSupportedException e) {
            // Handle the exception as needed
            return null;
        }
    }
}
