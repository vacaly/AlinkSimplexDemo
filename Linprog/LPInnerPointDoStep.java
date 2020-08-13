package com.alibaba.alink.Linprog;

import com.alibaba.alink.Linprog.util.LinProUtils;
import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import org.apache.flink.api.java.tuple.Tuple5;

import static com.alibaba.alink.Linprog.util.LinProUtils.Obj2DenseVector;

public class LPInnerPointDoStep extends ComputeFunction {
    @Override
    public void calc(ComContext context) {
        DenseVector x   =   Obj2DenseVector(LPInnerPointBatchOp.LOCAL_X,context,true);
        DenseVector y   =   Obj2DenseVector(LPInnerPointBatchOp.LOCAL_Y,context,true);
        DenseVector z   =   Obj2DenseVector(LPInnerPointBatchOp.LOCAL_Z,context,true);
        DenseVector d_x   =   context.getObj(LPInnerPointBatchOp.D_X);
        DenseVector d_y   =   context.getObj(LPInnerPointBatchOp.D_Y);
        DenseVector d_z   =   context.getObj(LPInnerPointBatchOp.D_Z);
        double tau  =   context.getObj(LPInnerPointBatchOp.LOCAL_TAU);
        double d_tau    =   context.getObj(LPInnerPointBatchOp.D_TAU);
        double kappa    =   context.getObj(LPInnerPointBatchOp.LOCAL_KAPPA);
        double d_kappa  =   context.getObj(LPInnerPointBatchOp.D_KAPPA);
        double alpha0   =   context.getObj(LPInnerPointBatchOp.ALPHA0);
        double alpha    = LinProUtils.getStep(x, d_x, z, d_z, tau,d_tau, kappa, d_kappa,alpha0);
        Tuple5<DenseVector, DenseMatrix, DenseVector, Double, Double> data = LinProUtils.doStep
                (x, d_x, y, d_y, z, d_z, tau, d_tau, kappa, d_kappa, alpha);
        context.putObj(LPInnerPointBatchOp.LOCAL_X, data.f0);
        context.putObj(LPInnerPointBatchOp.LOCAL_Y, data.f1);
        context.putObj(LPInnerPointBatchOp.LOCAL_Z, data.f2);
        context.putObj(LPInnerPointBatchOp.LOCAL_TAU, data.f3);
        context.putObj(LPInnerPointBatchOp.LOCAL_KAPPA, data.f4);
        context.putObj(LPInnerPointBatchOp.CONDITION_GO, indicators(x,y,z,tau, kappa,context));
    }

    static public boolean indicators(DenseVector x, DenseVector y, DenseVector z,
                                     double tau, double kappa,
                                     ComContext context){
        DenseVector r_p0_vec = context.getObj(LPInnerPointBatchOp.R_P0);
        DenseVector r_d0_vec = context.getObj(LPInnerPointBatchOp.R_D0);
        double r_p0 = r_p0_vec.normL2();
        double r_d0 = r_d0_vec.normL2();
        double r_g0 = context.getObj(LPInnerPointBatchOp.R_G0);
        //double mu_0 = context.getObj(LPInnerPointBatchOp.MU_0);
        //double c0   = context.getObj(LPInnerPointBatchOp.STATIC_C0);
        DenseMatrix A = context.getObj(LPInnerPointBatchOp.MATRIX);
        DenseVector b = Obj2DenseVector(LPInnerPointBatchOp.STATIC_B, context, false);
        DenseVector c = Obj2DenseVector(LPInnerPointBatchOp.STATIC_C, context, false);
        double rho_A = Math.abs(c.dot(x) - b.dot(y)) / (tau + Math.abs(b.dot(y)));
        double rho_p = b.scale(tau).minus(A.multiplies(x)).normL2();
        rho_p = r_p0 > 1 ? rho_p / r_p0 : rho_p;
        double rho_d = c.scale(tau).minus(A.transpose().multiplies(y)).minus(z).normL2();
        rho_d = r_d0 > 1 ? rho_d / r_d0 : rho_d;
        double rho_g = kappa + c.dot(x) - b.dot(y);
        rho_g = r_g0 > 1 ? rho_g / r_g0 : rho_g;
        //double rho_mu= (x.dot(z) + tau * kappa) / ((x.size()+1)*mu_0);
        //double obj   = x.scale(1/tau).dot(c) + c0;
        double tol   = 1e-8;
        if(rho_p>tol || rho_d>tol || rho_A>tol)
            return true;
        return false;
    }
}
