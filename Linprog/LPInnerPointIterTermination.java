package com.alibaba.alink.Linprog;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CompareCriterionFunction;

public class LPInnerPointIterTermination extends CompareCriterionFunction {
    @Override
    public boolean calc(ComContext context) {
        boolean go   =   context.getObj(LPInnerPointBatchOp.CONDITION_GO);
        return go;
    }
}