package com.alibaba.alink.devp;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CompleteResultFunction;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.utils.RowCollector;
import org.apache.flink.types.Row;

import java.util.List;

public class LPSimplexComplete extends CompleteResultFunction {
    @Override
    public List<Row> calc(ComContext context) {
        DenseVector object = context.getObj(LPSimplexBatchOp.OBJECTIVE);
        Row row = new Row(1);
        row.setField(0, -1*object.get(0));
        RowCollector collector = new RowCollector();
        collector.collect(row);
        return collector.getRows();
    }
}
