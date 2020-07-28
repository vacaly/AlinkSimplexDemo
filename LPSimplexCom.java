package com.alibaba.alink.devp;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.common.linalg.DenseVector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Double.MAX_VALUE;

public class LPSimplexCom extends ComputeFunction {
    public static final double[] invalidRow = {-1,-1};
    @Override
    public void calc(ComContext context) {
        DenseVector objectiveRow;
        DenseVector pseudoObjectiveRow;
        List<Tuple2<Integer,DenseVector>> tableau;
        int phase;
        int basisNum;
        int nextPivotCol;
        int nextPivotRow;

        if(context.getStepNo()==1){
            /**
             * Apply pivot in other steps and init some dense vector int first step.
             * */
            phase = 1;
            context.putObj(LPSimplexBatchOp.PHASE,phase);
            List<Row> tableauRow        =   context.getObj(LPSimplexBatchOp.TABLEAU);
            List<Row> objective         =   context.getObj(LPSimplexBatchOp.OBJECTIVE);
            List<Row> basis             =   context.getObj(LPSimplexBatchOp.BASIS);
            List<Row> pseudoObjective   =   context.getObj(LPSimplexBatchOp.PSEUDO_OBJECTIVE);
            objectiveRow                =   new DenseVector(objective.size());
            basisNum                    =   basis.size();
            pseudoObjectiveRow          =   new DenseVector(pseudoObjective.size());

            for(int i=0;i<objective.size();i++)
                objectiveRow.set(i,(double)objective.get(i).getField(0));
            for(int i=0;i<pseudoObjective.size();i++)
                pseudoObjectiveRow.set(i,(double)pseudoObjective.get(i).getField(0));

            context.putObj(LPSimplexBatchOp.OBJECTIVE,objectiveRow);
            context.putObj(LPSimplexBatchOp.PSEUDO_OBJECTIVE,pseudoObjectiveRow);
            context.putObj(LPSimplexBatchOp.BASIS,basisNum);
            context.putObj(LPSimplexBatchOp.UNBOUNDED,false);
            context.putObj(LPSimplexBatchOp.COMPLETE,false);

            ArrayList<Tuple2<Integer,DenseVector>> tmp = new ArrayList<>();
            for(int i=0;i<tableauRow.size();i++) {
                tmp.add((Tuple2<Integer, DenseVector>) tableauRow.get(i).getField(0));
            }
            tableau = tmp;
            context.putObj(LPSimplexBatchOp.TABLEAU, tableau);
        } else {
            tableau                 =   context.getObj(LPSimplexBatchOp.TABLEAU);
            phase                   =   context.getObj(LPSimplexBatchOp.PHASE);
            objectiveRow            =   context.getObj(LPSimplexBatchOp.OBJECTIVE);
            pseudoObjectiveRow      =   context.getObj(LPSimplexBatchOp.PSEUDO_OBJECTIVE);
            basisNum                =   context.getObj(LPSimplexBatchOp.BASIS);
            int enteringVar         =   context.getObj(LPSimplexBatchOp.PIVOT_COL_INDEX);
            double[] pivotRowList   =   context.getObj(LPSimplexBatchOp.PIVOT_ROW_VALUE);
            int leavingVar          =   (int)pivotRowList[0];
            DenseVector pivotRow    =   list2vector(pivotRowList);

            tableau             =   applyPivot(tableau,pivotRow,enteringVar,leavingVar);
            objectiveRow        =   applyPivot(objectiveRow,pivotRow,enteringVar);
            pseudoObjectiveRow  =   applyPivot(pseudoObjectiveRow,pivotRow,enteringVar);

            context.putObj(LPSimplexBatchOp.TABLEAU,tableau);
            context.putObj(LPSimplexBatchOp.OBJECTIVE,objectiveRow);
            context.putObj(LPSimplexBatchOp.PSEUDO_OBJECTIVE,pseudoObjectiveRow);

            if(context.getTaskId()==0) {
                System.out.printf("step %d start leave %d enter %d, phase %d\n",context.getStepNo(),leavingVar,enteringVar,phase);
                System.out.println("-----------------------tableau--------------------------");
                linprogUtil.LPPrintTableau(tableau);
                linprogUtil.LPPrintVector(objectiveRow);
                linprogUtil.LPPrintVector(pseudoObjectiveRow);
            }
        }

        /**
         * Same leaving variable on different workers.
         */
        if(phase==1)
            nextPivotCol = enterVariableSelection(pseudoObjectiveRow, false, 0, context);
        else
            nextPivotCol = enterVariableSelection(objectiveRow, false, basisNum, context);
        if(nextPivotCol==-1 && phase==1){
            //phase 1 ends. phase 2 starts
            context.putObj(LPSimplexBatchOp.PHASE, 2);
            nextPivotCol = enterVariableSelection(objectiveRow, false, basisNum, context);
        }
        if(nextPivotCol!=-1)
            context.putObj(LPSimplexBatchOp.PIVOT_COL_INDEX, nextPivotCol);
        else
            context.putObj(LPSimplexBatchOp.COMPLETE,true);
        /**
         * Different entering variable on different workers.
         */
        nextPivotRow = leaveVariableSelection(tableau,nextPivotCol,context);
        if(nextPivotRow==-1) {
            context.putObj(LPSimplexBatchOp.UNBOUNDED, true);
            context.putObj(LPSimplexBatchOp.PIVOT_ROW_VALUE, invalidRow);
        } else{
            context.putObj(LPSimplexBatchOp.PIVOT_ROW_VALUE,
                    vector2list(tableau.get(nextPivotRow),nextPivotCol));
        }
    }

    private int enterVariableSelection(DenseVector objectiveRow, Boolean blade, Integer reservedRange, ComContext context) {
        double[] rowData = objectiveRow.getData();
        int minColIndex = 0;
        double minColValue = 0;
        for(int i=1;i<rowData.length-reservedRange;i++){
            if(rowData[i]<0 && Math.abs(rowData[i])>1e-6 && rowData[i]<=minColValue){
                if(blade){
                    return i-1;
                }
                minColIndex = i;
                minColValue = rowData[i];
            }
        }
        return minColIndex-1;
    }

    private int leaveVariableSelection(List<Tuple2<Integer,DenseVector>> tableau,Integer pivotCol, ComContext context){
        if(pivotCol==-1)
            return -1;
        int minRowIndex = -1;
        double minRowValue = MAX_VALUE;
        int i=0;
        for(Tuple2<Integer,DenseVector> t : tableau){
            DenseVector rowValue = t.f1;
            double q;
            if(rowValue.get(pivotCol+1)>0) {
                q = rowValue.get(0) / rowValue.get(pivotCol+1);
                if(q < minRowValue && q > 0){
                    minRowIndex = i;
                    minRowValue = q;
                }
            }
            i++;
        }
        return minRowIndex;
    }

    private double[] vector2list(Tuple2<Integer,DenseVector> row, Integer pivotCol){
        if(pivotCol==-1)
            return invalidRow;
        DenseVector scaledRow = row.f1.scale(1/row.f1.get(pivotCol+1));
        return scaledRow.prefix((double)row.f0).getData();
    }

    private DenseVector list2vector(double[] pivotRow){
        DenseVector pivotRowValue = new DenseVector(pivotRow.length-1);
        for(int i=0;i<pivotRow.length-1;i++)
            pivotRowValue.set(i,pivotRow[i+1]);
        return pivotRowValue;
    }

    private List<Tuple2<Integer,DenseVector>> applyPivot(List<Tuple2<Integer,DenseVector>> tableau,
                                                         DenseVector pivotRow,
                                                         Integer enteringVar, Integer leavingVar){
        for(Tuple2<Integer,DenseVector> t: tableau){
            double c = t.f1.get(enteringVar+1);
            if(t.f0!=leavingVar && c!=0){
                assert t.f1.size()==pivotRow.size();
                t.f1.minusEqual(pivotRow.scale(c));
            }else if(t.f0==leavingVar){
                t.f0 = enteringVar;
                t.f1 = pivotRow;
            }
        }
        return tableau;
    }

    private DenseVector applyPivot(DenseVector objective, DenseVector pivotRow,
                                   Integer enteringVar){
        double c = objective.get(enteringVar+1);
        objective.minusEqual(pivotRow.scale(c));
        return objective;
    }

}