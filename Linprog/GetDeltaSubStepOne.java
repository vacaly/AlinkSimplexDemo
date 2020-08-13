package com.alibaba.alink.Linprog;

import com.alibaba.alink.Linprog.util.LinProUtils;
import com.alibaba.alink.Linprog.util.appendSlack;
import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;

import com.alibaba.alink.common.linalg.BLAS;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.alink.Linprog.util.LinProUtils.VectorPlusConst;

public class GetDeltaSubStepOne extends ComputeFunction {
    @Override
    public void calc(ComContext context) {
        int m;
        int n;
        int[]   rangeM;
        int[]   rangeN;
        //calculation resources
        DenseMatrix matrix_A;
        double[] vector_b;
        double[] vector_c;
        double  c0;
        double[] vector_x;
        double[] vector_y;
        double[] vector_z;
        double  tau;
        double  kappa;
        //calculation result
        double[] r_P;
        double[] r_D;
        double[] M;
        double[] x_div_z;
        double mu = 0.0;
        double r_G= 0.0;

        if(context.getStepNo()==1){
            List<Row> fullMatrixListRow     = context.getObj(LPInnerPointBatchOp.MATRIX);
            List<Row> coefficientListRow    = context.getObj(LPInnerPointBatchOp.VECTOR);
            List<Row> upperBoundsListRow    = context.getObj(LPInnerPointBatchOp.UPPER_BOUNDS);
            List<Row> lowerBoundsListRow    = context.getObj(LPInnerPointBatchOp.LOWER_BOUNDS);
            List<Row> unBoundsListRow       = context.getObj(LPInnerPointBatchOp.UN_BOUNDS);

            //process original input to standard form
            List<Tuple2<Integer, DenseVector>> fullMatrixTupleIntVec = null;
            DenseVector objectiveRow = null;
            try {
                Tuple2<List<Tuple2<Integer, DenseVector>>, DenseVector> data0   =   appendSlack.append(fullMatrixListRow,coefficientListRow,upperBoundsListRow,lowerBoundsListRow,unBoundsListRow);
                fullMatrixTupleIntVec   =   data0.f0;
                objectiveRow    =   data0.f1;
            } catch (Exception e) {
                e.printStackTrace();
            }

            m   =   fullMatrixTupleIntVec.size();
            n   =   objectiveRow.size() - 1;

            //A, b, c are constant
            matrix_A    =   new DenseMatrix(m,n);
            vector_b    =   new double[m];
            vector_c    =   new double[n];
            for(int i=0;i<m;i++){
                vector_b[i] =   fullMatrixTupleIntVec.get(i).f1.get(0);
                for(int j=0;j<n;j++)
                    matrix_A.set(i,j,fullMatrixTupleIntVec.get(i).f1.get(j+1));
            }
            for(int j=0;j<n;j++)
                vector_c[j] =   objectiveRow.get(j+1);
            c0   =   objectiveRow.get(0);

            //initialize x, y, z, tau, kappa, then putObj
            double[][] initXYZ  = LinProUtils.getBlindStart(m+2,n+2);//first two elements to store start tail info
            rangeM  =   LinProUtils.getStartTail(m,context.getTaskId(),context.getNumTask());
            rangeN  =   LinProUtils.getStartTail(n,context.getTaskId(),context.getNumTask());
            vector_x    =   initXYZ[0];
            vector_y    =   initXYZ[1];
            vector_z    =   initXYZ[2];
            tau         =   initXYZ[3][0];
            kappa       =   initXYZ[4][0];
            vector_x[0] =   rangeM[0];
            vector_x[1] =   rangeM[1];
            vector_y[0] =   rangeN[0];
            vector_y[1] =   rangeN[1];
            vector_z[0] =   rangeM[0];
            vector_z[1] =   rangeM[1];

            context.putObj(LPInnerPointBatchOp.STATIC_A, matrix_A);
            context.putObj(LPInnerPointBatchOp.STATIC_B, vector_b);
            context.putObj(LPInnerPointBatchOp.STATIC_C, vector_c);
            context.putObj(LPInnerPointBatchOp.STATIC_C0, c0);
            context.putObj(LPInnerPointBatchOp.LOCAL_X, vector_x);
            context.putObj(LPInnerPointBatchOp.LOCAL_Y, vector_y);
            context.putObj(LPInnerPointBatchOp.LOCAL_Z, vector_z);
            context.putObj(LPInnerPointBatchOp.LOCAL_TAU, tau);
            context.putObj(LPInnerPointBatchOp.LOCAL_KAPPA, kappa);
            context.putObj(LPInnerPointBatchOp.R_P0,
                    (new DenseVector(vector_b))
                    .minus(matrix_A.multiplies(DenseVector.ones(n)))
            );
            context.putObj(LPInnerPointBatchOp.R_D0,
                    (new DenseVector(vector_c)
                    .minus(DenseVector.ones(n)))
            );
            context.putObj(LPInnerPointBatchOp.R_G0, BLAS.asum(new DenseVector(vector_c))+1);
            context.putObj(LPInnerPointBatchOp.MU_0, (n+1)/(m+1));

        }else{
            matrix_A    =   context.getObj(LPInnerPointBatchOp.STATIC_A);
            vector_x    =   context.getObj(LPInnerPointBatchOp.LOCAL_X);
            vector_y    =   context.getObj(LPInnerPointBatchOp.LOCAL_Y);
            vector_z    =   context.getObj(LPInnerPointBatchOp.LOCAL_Z);
            vector_b    =   context.getObj(LPInnerPointBatchOp.STATIC_B);
            vector_c    =   context.getObj(LPInnerPointBatchOp.STATIC_C);
            tau         =   context.getObj(LPInnerPointBatchOp.LOCAL_TAU);
            kappa       =   context.getObj(LPInnerPointBatchOp.LOCAL_KAPPA);
            m   =   matrix_A.numRows();
            n   =   matrix_A.numCols();
            rangeM  =   LinProUtils.getStartTail(m,context.getTaskId(),context.getNumTask());
            rangeN  =   LinProUtils.getStartTail(n,context.getTaskId(),context.getNumTask());
        }

        //to calculate r_P, r_D, r_G
        r_P = new double[m+2];
        r_D = new double[n+2];
        Arrays.fill(r_P,0.0);
        Arrays.fill(r_D,0.0);

        r_P[0]  =   (double)rangeM[0];
        r_P[1]  =   (double)rangeM[1];
        r_D[0]  =   (double)rangeN[0];
        r_D[1]  =   (double)rangeN[1];

        // r_P = b * tau - A.dot(x)
        for(int i = rangeM[0] ; i < rangeM[1] ; i++) {
            for (int j = 0; j < n; j++)
                r_P[i + 2] -= matrix_A.get(i, j) * vector_x[j + 2];
            r_P[i+2] += vector_b[i] * tau;
        }
        for(int i = rangeN[0] ; i < rangeN[1] ; i++) {
            for (int j = 0; j < m; j++)
                r_D[i + 2] -= matrix_A.get(j, i) * vector_y[j + 2];
            r_D[i+2] += vector_c[i] * tau - vector_z[i+2];
        }
        for(int i = 0 ; i < n ; i++) {
            mu  += vector_x[i + 2] * vector_z[i + 2];
            r_G += vector_x[i + 2] * vector_c[i];
        }
        for(int i = 0 ; i < m ; i++)
            r_G -= vector_y[i+2] * vector_b[i];
        mu  = (mu + (tau * kappa)) / (n + 1);
        r_G += kappa;
        context.putObj(LPInnerPointBatchOp.R_P, r_P);
        context.putObj(LPInnerPointBatchOp.R_D, r_D);
        context.putObj(LPInnerPointBatchOp.LOCAL_MU, mu);
        context.putObj(LPInnerPointBatchOp.R_G, r_G);

        //to calculate M, 2D square matrix
        M   = new double[m*m+2];
        x_div_z = new double[n];
        Arrays.fill(M, 0.0);
        M[0]= rangeM[0] * m;
        M[1]= rangeM[1] * m;

        //M = A.dot( x/z * A.T)
        DenseMatrix A_T = matrix_A.clone();
        for(int i = 0; i < n; i++) {
            double scale = vector_x[i+2]/vector_z[i+2];
            x_div_z[i] = scale;
            for (int j = 0; j < m; j++)
                A_T.set(j, i, A_T.get(j, i) * scale);
        }
        //M_sub shape is (part_m, n)
        double[] M_sub = A_T.getSubMatrix(rangeM[0], rangeM[1], 0, n)
                .multiplies(A_T.transpose())
                .getArrayCopy1D(true);
        System.arraycopy(M_sub, 0, M, (int)M[0], (int)(M[1]-M[0]));

        context.putObj(LPInnerPointBatchOp.LOCAL_M, M);
        context.putObj(LPInnerPointBatchOp.LOCAL_X_DIV_Z, x_div_z);
    }
}
