package com.trilobita.runtime.launcher.impl;

import com.trilobita.commons.Computable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.math.BigInteger;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DoubleComputable implements Computable<BigDecimal> {
    private BigDecimal value;
    public BigDecimal getValue(){
        return this.value;
    }
    @Override
    public Computable<BigDecimal> add(Computable<BigDecimal> computable) {
        DoubleComputable doubleComputable = (DoubleComputable) computable;
        this.value = this.value.add(doubleComputable.getValue());
        return this;
    }

    @Override
    public Computable<BigDecimal> minus(Computable<BigDecimal> computable) {
        DoubleComputable doubleComputable = (DoubleComputable) computable;
        this.value=this.value.subtract(doubleComputable.getValue());
        return this;
    }

    @Override
    public Computable<BigDecimal> multiply(Computable<BigDecimal> computable) {
        DoubleComputable doubleComputable = (DoubleComputable) computable;
        this.value = this.value.multiply(doubleComputable.getValue());
        return this;
    }


    public Computable<BigDecimal> multiply(double num) {
        this.value = this.value.multiply(BigDecimal.valueOf(num));
        return this;
    }
}
