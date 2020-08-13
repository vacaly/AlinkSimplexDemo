package com.alibaba.alink.Linprog;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CompleteResultFunction;
import com.alibaba.alink.common.utils.RowCollector;
import org.apache.flink.types.Row;

import java.util.List;

import static com.alibaba.alink.Linprog.LPInnerPointBatchOp.VECTOR;

public class MatMulVecComplete extends CompleteResultFunction {
    @Override
    public List<Row> calc(ComContext context) {
        double[] vec = context.getObj(VECTOR);
        if(context.getTaskId()==0) {
            for (int i = 2; i < vec.length; i++)
                System.out.printf("%.2f,\t", vec[i]);
            System.out.println();
        }

        Row row = new Row(1);
        row.setField(0, context.getTaskId());
        RowCollector collector = new RowCollector();
        collector.collect(row);
        return collector.getRows();
    }
}
