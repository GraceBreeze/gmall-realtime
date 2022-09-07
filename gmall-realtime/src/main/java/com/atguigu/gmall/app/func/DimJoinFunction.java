package com.atguigu.gmall.app.func;

import com.alibaba.fastjson.JSONObject;

/**
 * @author yhm
 * @create 2022-08-30 9:27
 */
public interface DimJoinFunction<T> {
    public abstract String getKey(T input);
    public abstract void join(T input, JSONObject obj);
}
