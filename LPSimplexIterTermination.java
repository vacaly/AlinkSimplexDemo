package com.alibaba.alink.devp;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CompareCriterionFunction;

public class LPSimplexIterTermination extends CompareCriterionFunction {
    @Override
    public boolean calc(ComContext context) {
        double[] pivotRowList   =   context.getObj(LPSimplexBatchOp.PIVOT_ROW_VALUE);
        if(pivotRowList[0]<0) {
            System.out.println("completed");
            return true;
        }
        return false;
    }
}
