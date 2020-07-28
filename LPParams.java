package com.alibaba.alink.devp;

import com.alibaba.alink.common.linalg.SparseVector;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import java.util.ArrayList;

public interface LPParams<T> extends WithParams<T>{
    ParamInfo<ArrayList> OBJECTIVE = ParamInfoFactory
            .createParamInfo("objective", ArrayList.class)
            .setDescription("demo param")
            .build();
    ParamInfo<SparseVector> UPPER_BOUNDS = ParamInfoFactory
            .createParamInfo("upperBounds", SparseVector.class)
            .setHasDefaultValue(null)
            .build();
    ParamInfo<SparseVector> LOWER_BOUNDS = ParamInfoFactory
            .createParamInfo("lowerBounds", SparseVector.class)
            .setHasDefaultValue(null)
            .build();
    ParamInfo<Integer[]> UNBOUNDED = ParamInfoFactory
            .createParamInfo("unbounded", Integer[].class)
            .setHasDefaultValue(null)
            .build();
    ParamInfo<Integer> N = ParamInfoFactory
            .createParamInfo("n", Integer.class)
            .setDescription("number of true variables in the problem")
            .build();
    ParamInfo<Integer> M = ParamInfoFactory
            .createParamInfo("m", Integer.class)
            .setDescription("number of conditions in the problem")
            .build();
    ParamInfo <Integer> MAX_ITER = ParamInfoFactory
            .createParamInfo("maxIter", Integer.class)
            .setDescription("Maximum iterations, the default value is 50")
            .setHasDefaultValue(4)
            .setAlias(new String[] {"numIter"})
            .build();

    default ArrayList getObjective() {return get(OBJECTIVE);}
    default SparseVector getUpperBounds() {return get(UPPER_BOUNDS);}
    default SparseVector getLowerBounds() {return get(LOWER_BOUNDS);}
    default Integer[] getUnbounded() {return get(UNBOUNDED);}
    default Integer getN() {return get(N);}
    default Integer getM() {return get(M);}
    default Integer getMaxIter() {return get(MAX_ITER);}

    default T setObjective(ArrayList value) {return set(OBJECTIVE, value);}
    default T setUpperBounds(SparseVector value) {return set(UPPER_BOUNDS, value);}
    default T setLowerBounds(SparseVector value) {return set(LOWER_BOUNDS, value);}
    default T setUnbounded(Integer[] value) {return set(UNBOUNDED, value);}
    default T setN(Integer value) {return set(N, value);}
    default T setM(Integer value) {return set(M, value);}
    default T setMaxIter(Integer value) {return set(MAX_ITER, value);}
}
