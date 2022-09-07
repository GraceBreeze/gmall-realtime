package com.atguigu.gmall.util;


import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author yhm
 * @create 2022-08-29 15:29
 */
public class ThreadPoolUtil {
    private static ThreadPoolExecutor threadPoolExecutor = null;
    static {
        threadPoolExecutor = new ThreadPoolExecutor(5, 20, 5 * 60L, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>());
    }

    public static ThreadPoolExecutor getThreadPoolExecutor(){
        return threadPoolExecutor;
    }

}
