package com.alibaba.alink.Linprog.util;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.devp.linprogUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.List;

public class appendArtificialVar {
    public static Tuple3<List<Tuple2<Integer,DenseVector>>, DenseVector, DenseVector> append(
            List<Tuple2<Integer,DenseVector>> tableau,
            DenseVector coefficients
    ) throws Exception {
        /** tableau         : List of (b, original var, slack var)
         *  coefficients    : (b, original var, slack var)
         *
         *  return Tableau, coefficients and pseudo objective coefficients with artificial variables
         * */
        int m = tableau.size();
        int n = coefficients.size();
        DenseVector pseudoObjective = DenseVector.zeros(m+n);
        coefficients = coefficients.concatenate(DenseVector.zeros(m));
        /**
         * add artificial variables
         * */
        for(int i = 0 ; i < m ; i++) {
            DenseVector d = tableau.get(i).f1.concatenate(DenseVector.zeros(m));
            if(d.get(0)<0)
                d = d.scale(-1);
            d.set(i+n,1);
            int idx = tableau.get(i).f0 + m;
            tableau.set(i, new Tuple2<>(idx, d));
            for(int j = 0 ; j < n+m ;j++)
                pseudoObjective.set(j,pseudoObjective.get(j) - d.get(j));
        }

        return new Tuple3<>(tableau,coefficients,pseudoObjective);
    }
}
