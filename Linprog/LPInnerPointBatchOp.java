package com.alibaba.alink.Linprog;

import com.alibaba.alink.common.comqueue.IterativeComQueue;
import com.alibaba.alink.common.comqueue.communication.AllReduce;
import com.alibaba.alink.common.comqueue.communication.AllReduce.SerializableBiConsumer;
import com.alibaba.alink.operator.batch.BatchOperator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

public class LPInnerPointBatchOp extends BatchOperator<LPInnerPointBatchOp> {
    public static final String MATRIX   =   "matrix";
    public static final String VECTOR   =   "vector";
    public static final String UPPER_BOUNDS =   "upperBounds";
    public static final String LOWER_BOUNDS =   "lowerBounds";
    public static final String UN_BOUNDS    =   "unBounds";
    public static final String PART_VECTOR  =   "partVector";
    public static final String RANGE    =   "range";

    public static final String STATIC_A =   "staticA";// (m,n)
    public static final String STATIC_B =   "staticB";// (m,)
    public static final String STATIC_C =   "staticC";// (n,)
    public static final String STATIC_C0=   "staticC0";// constant
    public static final String LOCAL_X  =   "localX";// (n+2,)
    public static final String LOCAL_Y  =   "localX";// (m+2,)
    public static final String LOCAL_Z  =   "localZ";// (n+2,)
    public static final String LOCAL_MU     =   "localMu";
    public static final String LOCAL_TAU    =   "localTau";
    public static final String LOCAL_KAPPA  =   "localKappa";
    public static final String LOCAL_GAMMA  =   "localGamma";
    public static final String LOCAL_M  =   "localM";// (m*m+2,)
    public static final String LOCAL_M_INV  =   "localMInv";// DenseMatrix
    public static final String LOCAL_X_DIV_Z=   "localXDivZ";// (n,)
    public static final String LOCAL_R_HAT_XS =   "localRHatXs";
    public static final String LOCAL_R_HAT_TK =   "localRHatTk";
    public static final String R_P  =   "r_P";// (m+2,)
    public static final String R_D  =   "r_D";// (n+2,)
    public static final String R_G  =   "r_G";// constant
    public static final String D_X=   "d_x";
    public static final String D_Y=   "d_y";
    public static final String D_Z=   "d_z";
    public static final String D_TAU    =   "d_tau";
    public static final String D_KAPPA  =   "d_kappa";
    public static final String ALPHA0=   "alpha0";
    public static final String R_P0 =   "r_p0";
    public static final String R_D0 =   "r_d0";
    public static final String R_G0 =   "r_g0";
    public static final String MU_0 =   "mu_0";
    public static final String CONDITION_GO =   "go";

    static DataSet<Row> iterateICQ(DataSet<Row> inputMatrix,
                                   DataSet<Row> inputVec){
        return new IterativeComQueue()
                .initWithBroadcastData(MATRIX,inputMatrix)
                .initWithBroadcastData(VECTOR,inputVec)
                .add(new GetDeltaSubStepOne())
                .add(new AllReduce(R_D, null, new mergeVectorReduceFunc()))
                .add(new AllReduce(R_P, null, new mergeVectorReduceFunc()))
                .add(new AllReduce(LOCAL_M, null, new mergeVectorReduceFunc()))
                .add(new GetDeltaSubStepOne())
                .add(new GetDeltaSolveStep(0))
                .add(new GetDeltaSolveStep(1))
                .add(new LPInnerPointDoStep())
                .setCompareCriterionOfNode0(new LPInnerPointIterTermination())
                .closeWith(new MatMulVecComplete())
                .setMaxIter(3)
                .exec();
    }

    @Override
    public LPInnerPointBatchOp linkFrom(BatchOperator<?>... inputs) {
        DataSet<Row> inputM = inputs[0].getDataSet();
        DataSet<Row> inputV = inputs[1].getDataSet();

        DataSet<Row> Input = iterateICQ(inputM,inputV)
                .map((MapFunction<Row, Row>)row->{
                    return row;
                })
                .returns(new RowTypeInfo(Types.INT));
        this.setOutput(Input, new String[]{"taskId"});

        return this;
    }

    private static class mergeVectorReduceFunc implements SerializableBiConsumer<double[], double[]> {
        @Override
        public void accept(double[] doubles, double[] doubles2) {
            int s = (int) doubles2[0];
            int t = (int) doubles2[1];
            System.arraycopy(doubles2,s+2,doubles,s+2,t-s);
        }
    }
}

