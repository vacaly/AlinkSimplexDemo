package com.alibaba.alink.devp;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.VectorIterator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class linprogUtil {
    /**
     *  Given a linear programming problem of the form:
     *     Minimize:
     *                  c @ x
     *     Subject to:
     *          A_ub @ x <= b_ub
     *          A_eq @ x == b_eq
     *             lb <= x <= ub
     *  where lb = 0 and ub = None when upperBounds/lowerBounds is null
     *  Return the problem in standard form:
     *     Minimize:
     *                  c @ x
     *     Subject to::
     *             A @ x == b
     *                 x >= 0
     *  by adding slack variables and making variable substitutions as necessary.
     *
     * @param constraints The value of constraints, expected form of Row is "a_i1 a_i2 ... a_in [le,ge,eq] b_i".
     * @param coefficients Coefficients of the linear objective function to be minimized.
     * @param upperBounds SparseVector for (x_id, max) pairs
     * @param lowerBounds SparseVector for (x_id, min) pairs
     * @param unbounded list of independent variables
     * @return tableau, objective row
     * */
    public static Tuple2<ArrayList<DenseVector>, ArrayList<Double>> appendSlack(
            List<Row> constraints,
            ArrayList<Double> coefficients,
            SparseVector upperBounds,   // can be null
            SparseVector lowerBounds,   // can be null
            Integer[] unbounded) throws Exception            // can be null
    {
        int n       =   constraints.get(0).getArity()-2;
        int n_free  =   unbounded == null ? 0 : unbounded.length;
        ArrayList<DenseVector> tableau   =   new ArrayList<>();

        for(Row row: constraints) {
            String relation = (String) row.getField(n);
            DenseVector constraint = DenseVector.zeros(n + n_free + 1);
            // set constraint
            for (int i = 1; i <= n; i++)
                constraint.set(i, (double) row.getField(i - 1));
            // set RHS
            constraint.set(0, (double) row.getField(n + 1));
            // unbounded: substitute xi = xi+ + xi-
            for (int i = 0; i < n_free; i++)
                constraint.set(n + i + 1, -constraint.get(1 + unbounded[i]));
            // assess relation
            switch (relation) {
                case "le":
                    tableau.add(constraint);
                    break;
                case "ge":
                    // a @ x >= b : substitute xi = -xi'
                    tableau.add(constraint.scale(-1));
                    break;
                case "eq":
                    // a @ x == b : substitute a @ x <= b && a @ x >= b
                    tableau.add(constraint);
                    tableau.add(constraint.scale(-1));
                    break;
                default:
                    System.out.printf("unexpected relation %s\n", relation);
                    break;
            }
        }

        if(upperBounds!=null) {
            for(VectorIterator upperBound = upperBounds.iterator();upperBound.hasNext();upperBound.next()) {
                DenseVector constraint = DenseVector.zeros(n + n_free + 1);
                constraint.set(1 + upperBound.getIndex(), upperBound.getValue());
                constraint.set(1 + upperBound.getIndex(), 1.0);
                tableau.add(constraint);
            }
        }

        if(lowerBounds!=null){
            for(VectorIterator lowerBound = lowerBounds.iterator();lowerBound.hasNext();lowerBound.next()) {
                int i = lowerBound.getIndex();
                double bound = lowerBound.getValue();
                for(DenseVector t: tableau){
                    double a_ti = t.get(1+i);
                    double b_t = t.get(0);
                    t.set(0, b_t - bound*a_ti);
                }
                double c_0 = coefficients.get(0);
                double c_i = coefficients.get(1+i);
                coefficients.set(0,c_0 - bound*c_i);
            }
        }

        int m = tableau.size();
        n = n + n_free;
        // add slack on tableau
        for(int i = 0 ; i < m ; i++){
            DenseVector d = tableau.get(i).concatenate(new DenseVector(m));
            d.set(n+i+1 , 1.0);
            tableau.set(i,d);
        }
        // add substituted xi on coefficients
        for (int i = 0; i < n_free; i++)
            coefficients.add(-coefficients.get(1 + unbounded[i]));
        // add slack on coefficients
        for(int i = 0 ; i < m ; i++)
            coefficients.add(0.0);

        return new Tuple2<>(tableau,coefficients);
    }

    public static Tuple3<DataSet<Row>, DataSet<Row>, DataSet<Row>> addSlack(
            DataSet<Row> input,
            ArrayList<Double> coefficients,
            SparseVector upperBounds,   // can be null
            SparseVector lowerBounds,   // can be null
            Integer[] unbounded) throws Exception            // can be null
    {
        List<Row> constraints = input.collect();
        coefficients.add(0,0.0);

        Tuple2<ArrayList<DenseVector>, ArrayList<Double>> data = appendSlack(
                constraints, coefficients, upperBounds, lowerBounds, unbounded);

        ArrayList<DenseVector> tableauList = data.f0;
        Object[] objectiveRow = data.f1.toArray();
        ArrayList<Tuple2<Integer,DenseVector>> tableauRows   =   new ArrayList<>();
        int m = tableauList.size();
        int n = objectiveRow.length-1;
        Object[] basisRow = new Integer[m];

        for(int i = 0 ; i < m ; i++) {
            basisRow[i] = i + n - m;
            tableauRows.add(new Tuple2<>(i+n-m,tableauList.get(i)));
        }

        //Tuple2<Integer,DenseVector>
        DataSet<Row> tableau = new MemSourceBatchOp(tableauRows.toArray(), "tableau").getDataSet().rebalance();
        //Double
        DataSet<Row> objective = new MemSourceBatchOp(objectiveRow,"objective").getDataSet();
        //Double
        DataSet<Row> basis = new MemSourceBatchOp(basisRow,"objective").getDataSet();

        return new Tuple3<>(tableau,objective,basis);
    }

    /**
     * Given a linear programming problem return the problem in standard form
     * by adding slack variables. 'A', the 2D array, such that ``A`` @ ``x`` gives
     * the values of the equality constraints at ``x``. 'b' represents the RHS of
     * each equality constraint (row) in A (for standard form problem). 'c' is
     * coefficient of the linear objective function to be minimized.
     * +---+------------------------Tableau---------------------------+
     * |   |       org_var      ;    slack_var     ;  artificial_var  |
     * |   |                                                          |
     * | b |                            A                             |
     * |   |                                                          |
     * |   |                                                          |
     * +---+----------------------------------------------------------+
     * |                                c                             |
     * +--------------------------------------------------------------+
     *
     * @param input The value of constraints, expected form of Row is "a_i1 a_i2 ... a_in [le,ge,eq] b_i".
     * @param coefficients Coefficients of the linear objective function to be minimized.
     * @param upperBounds SparseVector for (x_id, max) pairs
     * @param lowerBounds SparseVector for (x_id, min) pairs
     * @param unbounded list of independent variables
     * @return tableau, objective row, pseudo objective row, basis row
     * */
    public static Tuple4<DataSet<Row>, DataSet<Row>, DataSet<Row>, DataSet<Row>> addArtificialVar(
            DataSet<Row> input,
            ArrayList<Double> coefficients,
            SparseVector upperBounds,   // can be null
            SparseVector lowerBounds,   // can be null
            Integer[] unbounded) throws Exception            // can be null
    {
        List<Row> constraints = input.collect();
        coefficients.add(0,0.0);

        /**
         * add slack variables
         * */
        Tuple2<ArrayList<DenseVector>, ArrayList<Double>> data = appendSlack(
                constraints, coefficients, upperBounds, lowerBounds, unbounded);

        ArrayList<DenseVector> tableauList = data.f0;
        ArrayList<Tuple2<Integer,DenseVector>> tableauRows   =   new ArrayList<>();
        int m = tableauList.size();
        int n = tableauList.get(0).size() - 1;
        Object[] objectiveRow       =   new Double[n+m+1];
        Object[] basisRow           =   new Integer[m];
        Double[] pseudoObjectiveRow =   new Double[n+m+1];
        Arrays.fill(objectiveRow,0.0);
        Arrays.fill(pseudoObjectiveRow,0.0);
        System.arraycopy(data.f1.toArray(), 0, objectiveRow, 0, n+1);

        /**
         * add artificial variables
        * */
        for(int i = 0 ; i < m ; i++) {
            basisRow[i] = i + n ;
            DenseVector d = tableauList.get(i).concatenate(DenseVector.zeros(m));
            if(d.get(0)<0)
                d = d.scale(-1);
            d.set(i+n+1,1);
            tableauRows.add(new Tuple2<>(i+n,d));
            LPPrintVector(d);
            for(int j = 0 ; j <= n ;j++)
                pseudoObjectiveRow[j] = pseudoObjectiveRow[j] - d.get(j);
        }

        //Row element is Tuple2<Integer,DenseVector>
        DataSet<Row> tableau = new MemSourceBatchOp(tableauRows.toArray(), "tableau")
                .getDataSet()
                .rebalance();

        //Row element is Double
        DataSet<Row> objective = new MemSourceBatchOp(objectiveRow,"objective")
                .getDataSet();

        DataSet<Row> pseudoObjective = new MemSourceBatchOp(pseudoObjectiveRow,"pseudoObjective")
                .getDataSet();

        DataSet<Row> basis = new MemSourceBatchOp(basisRow,"objective").getDataSet();

        return new Tuple4<>(tableau,objective,pseudoObjective,basis);
    }

    /**
     * print a vector
     * */
    public static void LPPrintVector(DenseVector vector){
        for(int i=0;i<vector.size();i++)
            System.out.printf("%.2f ",vector.get(i));
        System.out.print("\n");
    }

    /**
     * print the simplex tableau
     * */
    public static void LPPrintTableau(List<Tuple2<Integer,DenseVector>> tableau){
        for(Tuple2<Integer,DenseVector> t: tableau){
            System.out.printf("x_%d\t= %.2f\t",t.f0,t.f1.get(0));
        }

        System.out.println();

        for(Tuple2<Integer,DenseVector> t: tableau)
            LPPrintVector(t.f1);
    }
}
