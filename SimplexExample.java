package com.alibaba.alink.devp;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.operator.batch.BatchOperator;
import org.apache.flink.types.Row;

import java.util.ArrayList;

public class SimplexExample {

    public void test2() throws  Exception{
        Row[] rows1 = new Row[]{
                Row.of(-3.0,   1.0,   "le",   6.0),
                Row.of(1.0,    2.0,   "le",   4.0),
        };

        ArrayList<Double> objective = new ArrayList<>();
        objective.add(-1.0);
        objective.add(4.0);

        SparseVector lowerBounds = new SparseVector(1);
        lowerBounds.set(1, -3);

        Integer[] unbounded = new Integer[1];
        unbounded[0]    =   0;

        BatchOperator data = BatchOperator
                .fromTable(MLEnvironmentFactory
                        .getDefault()
                        .createBatchTable(rows1, new String[]{"x0", "x1", "relation", "b"}));

        LPSimplexBatchOp dOp = new LPSimplexBatchOp();

        dOp.setObjective(objective)
                .setLowerBounds(lowerBounds)
                .setUnbounded(unbounded)
                .setN(3)
                .setMaxIter(10)
                .setParallelism(1);

        dOp.linkFrom(data).collect();
    }
    public void test1() throws Exception {
        Row[] rows1 = new Row[]{
                Row.of(1.0,    -2.0,    1.0,    "le",   11.0),
                Row.of(-4.0,    1.0,    2.0,    "ge",   3.0),
                Row.of(-2.0,    0.0,    1.0,    "eq",   1.0),
        };

        ArrayList<Double> objective = new ArrayList<>();
        objective.add(-3.0);
        objective.add(1.0);
        objective.add(1.0);

        SparseVector lowerBounds = new SparseVector(1);
        lowerBounds.set(1, -3);

        Integer[] unbounded = new Integer[1];
        unbounded[0]    =   0;

        BatchOperator data = BatchOperator
                .fromTable(MLEnvironmentFactory
                        .getDefault()
                        .createBatchTable(rows1, new String[]{"x0", "x1", "x2", "relation", "b"}));

        LPSimplexBatchOp dOp = new LPSimplexBatchOp();

        dOp.setObjective(objective)
                //.setLowerBounds(lowerBounds)
                //.setUnbounded(unbounded)
                .setN(3)
                .setMaxIter(10)
                .setParallelism(1);

        dOp.linkFrom(data).collect();

    }

    public static void main(String[] args) throws Exception {
        SimplexExample readExample = new SimplexExample();

        readExample.test1();
    }
}
