package com.alibaba.alink.Linprog;

import com.alibaba.alink.common.comqueue.IterativeComQueue;
import com.alibaba.alink.common.comqueue.communication.AllReduce;
import com.alibaba.alink.devp.LPParams;
import com.alibaba.alink.devp.LPSimplexComplete;
import com.alibaba.alink.devp.LPSimplexIterTermination;
import com.alibaba.alink.operator.batch.BatchOperator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

public class LPSimplexBatchOp extends BatchOperator<LPSimplexBatchOp> implements LPParams<LPSimplexBatchOp> {
    public static final String TABLEAU = "tableau";
    public static final String UNBOUNDED = "unbounded";
    public static final String COMPLETED = "completed";
    public static final String M = "m";
    public static final String PIVOT_ROW_VALUE = "pivotRowValue"; //Tuple<Double, Integer, Integer, Vector> b/a, row index, task id, value of the pivot
    public static final String PIVOT_COL_INDEX = "pivotColIndex";
    public static final String PHASE = "phase";
    public static final String OBJECTIVE = "objective";
    public static final String PSEUDO_OBJECTIVE = "pseudoObjective";
    public static final String UPPER_BOUNDS = "upperBounds";
    public static final String LOWER_BOUNDS = "lowerBounds";
    public static final String UN_BOUNDS = "unBounds";

    static DataSet<Row> iterateICQ(DataSet <Row> Tableau,
                                   final int maxIter,
                                   DataSet<Row> Objective,
                                   DataSet<Row> UpperBounds,
                                   DataSet<Row> LowerBounds,
                                   DataSet<Row> UnBounds){
        return new IterativeComQueue()
                .initWithBroadcastData(TABLEAU, Tableau)
                .initWithBroadcastData(OBJECTIVE, Objective)
                .initWithBroadcastData(UPPER_BOUNDS, UpperBounds)
                .initWithBroadcastData(LOWER_BOUNDS, LowerBounds)
                .initWithBroadcastData(UN_BOUNDS, UnBounds)
                .add(new LPSimplexCom())
                .add(new AllReduce(PIVOT_ROW_VALUE, null,
                        new AllReduce.SerializableBiConsumer<double[], double[]>() {
                            @Override
                            public void accept(double[] a, double[] b) {
                                if(a[0]==-1.0 ||
                                        (b[0]>-1.0 && b[1]<a[1]) ||
                                        (b[1]==a[1] && b[0]<a[0]))
                                {
                                    for (int i = 0; i < a.length; ++i)
                                        a[i] = b[i];
                                }
                            }
                        }))
                .setCompareCriterionOfNode0(new LPSimplexIterTermination())
                .closeWith(new LPSimplexComplete())
                .setMaxIter(maxIter)
                .exec();
    }

    @Override
    public LPSimplexBatchOp linkFrom(BatchOperator<?>... inputs) {
        int inputLength                     = inputs.length;
        int maxIter                         = this.getMaxIter();
        if (inputLength <= 1) throw new AssertionError();
        DataSet<Row> TableauDataSet         = inputs[0].getDataSet();
        DataSet<Row> CoefficientsDataSet    = inputs[1].getDataSet();
        DataSet<Row> UpperBoundsDataSet     = inputLength>2 ? inputs[2].getDataSet() : null;
        DataSet<Row> LowerBoundsDataSet     = inputLength>3 ? inputs[3].getDataSet() : null;
        DataSet<Row> UnBoundsDataSet        = inputLength>4 ? inputs[4].getDataSet() : null;

        DataSet<Row> Input = iterateICQ(
                TableauDataSet,
                maxIter,
                CoefficientsDataSet,
                UpperBoundsDataSet,
                LowerBoundsDataSet,
                UnBoundsDataSet)
                .map((MapFunction<Row, Row>) row -> {
                    return row;
                })
                .returns(new RowTypeInfo(Types.DOUBLE));
        this.setOutput(Input, new String[]{"MinObject"});

        return this;
    }
}
