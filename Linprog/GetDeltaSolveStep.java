package com.alibaba.alink.Linprog;

import com.alibaba.alink.Linprog.util.LinProUtils;
import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;

import static com.alibaba.alink.Linprog.util.LinProUtils.*;

public class GetDeltaSolveStep extends ComputeFunction {
    int i_correction;
    public GetDeltaSolveStep(int i){
        i_correction = i;
    }

    @Override
    public void calc(ComContext context) {
        double  gamma;
        DenseMatrix M_inv;
        DenseMatrix matrix_A    =   context.getObj(LPInnerPointBatchOp.STATIC_A);
        DenseVector b   =   Obj2DenseVector(LPInnerPointBatchOp.STATIC_B,context,false);
        DenseVector c   =   Obj2DenseVector(LPInnerPointBatchOp.STATIC_C,context,false);
        DenseVector M1D =   Obj2DenseVector(LPInnerPointBatchOp.LOCAL_M,context,true);
        DenseVector x   =   Obj2DenseVector(LPInnerPointBatchOp.LOCAL_X,context,true);
        DenseVector y   =   Obj2DenseVector(LPInnerPointBatchOp.LOCAL_Y,context,true);
        DenseVector z   =   Obj2DenseVector(LPInnerPointBatchOp.LOCAL_Z,context,true);
        double  mu  =   context.getObj(LPInnerPointBatchOp.LOCAL_MU);
        double  tau =   context.getObj(LPInnerPointBatchOp.LOCAL_TAU);
        double kappa=   context.getObj(LPInnerPointBatchOp.LOCAL_KAPPA);
        DenseVector r_P =   Obj2DenseVector(LPInnerPointBatchOp.R_P,context,true);
        DenseVector r_D =   Obj2DenseVector(LPInnerPointBatchOp.R_D,context,true);
        DenseVector x_div_z =   Obj2DenseVector(LPInnerPointBatchOp.LOCAL_X_DIV_Z,context,false);
        int m   =   b.size();
        int n   =   c.size();
        int[] rangeN  = LinProUtils.getStartTail(n, context.getTaskId(), context.getNumTask());
        DenseMatrix M = new DenseMatrix(m, n, M1D.getData(), true);

        if(i_correction==0){
            gamma   =   0;
            M_inv = M.inverse();
            context.putObj(LPInnerPointBatchOp.LOCAL_M_INV, M_inv);
        }else {
            gamma = context.getObj(LPInnerPointBatchOp.LOCAL_GAMMA);
            M_inv = context.getObj(LPInnerPointBatchOp.LOCAL_M_INV);
        }

        DenseVector r;
        DenseVector r1;
        DenseVector v;
        DenseVector r_hat_xs;
        r   = b.plus(matrix_A.multiplies(VectorTimes(x_div_z, c)));
        DenseVector q = M_inv.multiplies(r);
        DenseVector p;
        p = matrix_A.transpose().multiplies(q).minus(c);
        p = VectorTimes(p, x_div_z);

        if(i_correction==0){
            r_hat_xs = VectorPlusConst(VectorTimes(x, z).scale(-1),gamma*mu);
            r1= r_D.minus(VectorDivs(r_hat_xs, x));
            r = r_P.plus(matrix_A.multiplies(VectorTimes(x_div_z, r1)));
            context.putObj(LPInnerPointBatchOp.LOCAL_R_HAT_XS,r_hat_xs);
        }else{
            DenseVector r_hat_p =   r_P.scale(1-gamma);
            DenseVector r_hat_d =   r_D.scale(1-gamma);
            DenseVector d_x =   context.getObj(LPInnerPointBatchOp.D_X);
            DenseVector d_z =   context.getObj(LPInnerPointBatchOp.D_Z);
            r_hat_xs = context.getObj(LPInnerPointBatchOp.LOCAL_R_HAT_XS);
            r_hat_xs.minusEqual(VectorTimes(d_x, d_z));
            r1 = r_hat_d.minus(VectorDivs(r_hat_xs, x));
            r = r_hat_p.plus(matrix_A.multiplies(VectorTimes(x_div_z, r1)));
        }
        v = M_inv.multiplies(r);
        DenseVector u;
        u = matrix_A.transpose().multiplies(v).minus(r1);
        u = VectorTimes(u, x_div_z);

        /* useless because this is not distributed calculation
        context.putObj(LPInnerPointBatchOp.REDUCED_Q, q);
        context.putObj(LPInnerPointBatchOp.REDUCED_P, p);
        context.putObj(LPInnerPointBatchOp.REDUCED_V, v);
        context.putObj(LPInnerPointBatchOp.REDUCED_U, u);
         */

        double r_G = c.dot(x) - b.dot(y) + kappa;
        double r_hat_g = gamma * r_G;
        double r_hat_tk= gamma * mu - tau * kappa;

        double d_tau = ((r_hat_g + 1 / tau * r_hat_tk - (-c.dot(u) + b.dot(v))) /
                (1 / tau * kappa + (-c.dot(p) + b.dot(q))));
        DenseVector d_x = p.scale(d_tau).plus(u);
        DenseVector d_y = q.scale(d_tau).plus(v);
        DenseVector d_z = VectorTimes(VectorDivs(DenseVector.ones(n), x), r_hat_xs.minus(VectorTimes(z, d_x)));
        double d_kappa  = 1/tau * (r_hat_tk - kappa * d_tau);
        double alpha    = LinProUtils.getStep(x, d_x, z,d_z, tau, d_tau, kappa,d_kappa, 1.0);
        gamma = (1-alpha)*(1-alpha) * Math.min(0.1,1-alpha);
        context.putObj(LPInnerPointBatchOp.LOCAL_GAMMA, gamma);
        context.putObj(LPInnerPointBatchOp.D_X,d_x);
        context.putObj(LPInnerPointBatchOp.D_Y,d_y);
        context.putObj(LPInnerPointBatchOp.D_Z,d_z);
        context.putObj(LPInnerPointBatchOp.D_TAU,d_tau);
        context.putObj(LPInnerPointBatchOp.D_KAPPA,d_kappa);
    }

}
