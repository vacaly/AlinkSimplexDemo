package com.alibaba.alink.devp;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CompareCriterionFunction;

public class LPSimplexIterTermination extends CompareCriterionFunction {
    @Override
    public boolean calc(ComContext context) {
        Boolean complete = context.getObj(LPSimplexBatchOp.COMPLETE);
        Boolean unbounded = context.getObj(LPSimplexBatchOp.UNBOUNDED);
        System.out.print(complete ? "complete, ": "not complete, ");
        System.out.println(unbounded && (!complete) ? "unbounded": "bounded");
        if(complete || unbounded) {
            return true;
        }
        return false;
    }
}
