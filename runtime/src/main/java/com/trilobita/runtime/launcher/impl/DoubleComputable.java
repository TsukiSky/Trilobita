package com.trilobita.runtime.launcher.impl;

import com.trilobita.commons.Computable;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class IntComparable implements Computable<Integer> {
    private int value;
    public int getValue(){
        return this.value;
    }
    @Override
    public Computable<Integer> add(Computable<Integer> computable) {
        IntComparable intComparable = (IntComparable) computable;
        this.value += intComparable.getValue();
        return this;
    }

    @Override
    public Computable<Integer> minus(Computable<Integer> computable) {
        IntComparable intComparable = (IntComparable) computable;
        this.value -= intComparable.getValue();
        return this;
    }

    @Override
    public Computable<Integer> multiply(Computable<Integer> computable) {
        IntComparable intComparable = (IntComparable) computable;
        this.value *= intComparable.getValue();
        return this;
    }

    public Computable<Integer> multiply(double num) {
        this.value *= num;
        return this;
    }
}
