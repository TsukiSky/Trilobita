package com.trilobita.examples.impl;

import com.trilobita.commons.Computable;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class PageRankValue extends Computable<BigDecimal> {
    public PageRankValue(BigDecimal value) {
        super(value);
    }

    @Override
    public Computable<BigDecimal> add(Computable<BigDecimal> computable) {
        this.value = this.value.add(computable.get());
        return this;
    }

    @Override
    public Computable<BigDecimal> minus(Computable<BigDecimal> computable) {
        this.value=this.value.subtract(computable.get());
        return this;
    }

    @Override
    public Computable<BigDecimal> multiply(Computable<BigDecimal> computable) {
        this.value = this.value.multiply(computable.get());
        return this;
    }

    public Computable<BigDecimal> multiply(double num) {
        this.value = this.value.multiply(BigDecimal.valueOf(num));
        return this;
    }

    @Override
    public Computable<BigDecimal> divide(Computable<BigDecimal> computable) {
        this.value = this.value.divide(computable.get(), RoundingMode.DOWN);
        return this;
    }

    @Override
    public BigDecimal get() {
        return this.value;
    }

    @Override
    public void set(BigDecimal value) {
        this.value = value;
    }
}
