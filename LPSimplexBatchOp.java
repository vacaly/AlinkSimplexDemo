package com.alibaba.alink.devp;

import com.alibaba.alink.common.comqueue.IterativeComQueue;
import com.alibaba.alink.common.comqueue.communication.AllReduce;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.operator.batch.BatchOperator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.util.ArrayList;

public class LPSimplexBatchOp extends BatchOperator<LPSimplexBatchOp> implements LPParams<LPSimplexBatchOp> {
    public static final String TABLEAU = "tableau";
    public static final String UNBOUNDED = "unbounded";
    public static final String COMPLETE = "complete";
    public static final String PIVOT_ROW_VALUE = "pivotRowValue"; //Tuple<Double, Integer, Integer, Vector> b/a, row index, task id, value of the pivot
    public static final String PIVOT_COL_INDEX = "pivotColIndex";
    public static final String PHASE = "phase";
    public static final String OBJECTIVE = "objective";
    public static final String PSEUDO_OBJECTIVE = "pseudoObjective";
    public static final String BASIS = "basis";

    public LPSimplexBatchOp(){
        super();
    }

    static DataSet<Row> iterateICQ(DataSet <Row> tableau,
                                   final int maxIter,
                                   DataSet<Row> objective,
                                   DataSet<Row> pseudoObjective,
                                   DataSet<Row> basis){
        return new IterativeComQueue()
                .initWithPartitionedData(TABLEAU, tableau)
                .initWithBroadcastData(OBJECTIVE, objective)
                .initWithBroadcastData(PSEUDO_OBJECTIVE, pseudoObjective)
                .initWithBroadcastData(BASIS, basis)
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
    public LPSimplexBatchOp linkFrom(BatchOperator <?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);
        int n = this.getN();
        ArrayList<Double> objective = this.getObjective();
        SparseVector lowerBounds = this.getLowerBounds();
        Integer[] unbounded = this.getUnbounded();

        DataSet<Row> Input = in.getDataSet();
        Tuple4<DataSet<Row>, DataSet<Row>, DataSet<Row>, DataSet<Row>> data = null;
        try {
            data = linprogUtil.addArtificialVar(Input, objective, null, lowerBounds, unbounded);
        } catch (Exception e) {
            e.printStackTrace();
        }

        assert data != null;
        Input = iterateICQ(data.f0,this.getMaxIter(), data.f1, data.f2, data.f3);

        DataSet <Row> iterOutput = Input
                .map((MapFunction<Row, Row>) row -> {
                    return row;
                }).returns(new RowTypeInfo(Types.DOUBLE));

        this.setOutput(iterOutput, new String[]{"MinObject"});

        return this;
    }

}
