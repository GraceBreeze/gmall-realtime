package com.atguigu.gmall.app.func;

import com.atguigu.gmall.util.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @author yhm
 * @create 2022-08-23 14:05
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public  class KeywordUDTF extends TableFunction<Row> {

    public void eval(String str) {
        List<String> stringList = KeywordUtil.analyze(str);
        for (String s : stringList) {
            // collect调用一次就是拆分一行  row里面都多数据就是多列
            collect(Row.of(s));
        }
    }
}
