package com.alibaba.alink.Linprog;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import org.apache.flink.types.Row;


public class LPSimplexExample {
    public void test1() throws Exception {
        Row[] r1 = new Row[]{
                Row.of(-3.0,   1.0,   "le",   6.0),
                Row.of(1.0,    2.0,   "le",   4.0)
        };

        Row[] r2 = new Row[]{
                Row.of(0, -1.0),
                Row.of(1, 4.0)
        };

        Row[] r3 = new Row[]{
                Row.of(1,300.0)
        };

        Row[] r4 = new Row[]{
                Row.of(1,-3.0)
        };

        Row[] r5 = new Row[]{
                Row.of(0)
        };

        BatchOperator op1 = BatchOperator.fromTable(MLEnvironmentFactory.getDefault()
                        .createBatchTable(r1, new String[]{"x0", "x1", "relation", "b"}));

        BatchOperator op2 = BatchOperator.fromTable(MLEnvironmentFactory.getDefault()
                .createBatchTable(r2, new String[]{"x", "c"}));

        BatchOperator op3 = BatchOperator.fromTable(MLEnvironmentFactory.getDefault()
                .createBatchTable(r3, new String[]{"x", "upperBounds"}));

        BatchOperator op4 = BatchOperator.fromTable(MLEnvironmentFactory.getDefault()
                .createBatchTable(r4, new String[]{"x", "lowerBounds"}));

        BatchOperator op5 = BatchOperator.fromTable(MLEnvironmentFactory.getDefault()
                .createBatchTable(r5, new String[]{"x",}));


        LPSimplexBatchOp dOp = new LPSimplexBatchOp();

        dOp.setMaxIter(8);
        BatchOperator.setParallelism(3);

        dOp.linkFrom(op1,op2,op3,op4,op5).print();
    }

    public static void main(String[] args) throws Exception {
        LPSimplexExample exp = new LPSimplexExample();
        exp.test1();
    }
}
