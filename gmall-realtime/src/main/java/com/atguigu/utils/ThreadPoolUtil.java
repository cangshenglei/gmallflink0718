package com.atguigu.utils;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {
    private static ThreadPoolExecutor threadPoolExecutor = null;

    private ThreadPoolUtil() {
    }

    public static ThreadPoolExecutor getThreadPoolExecutor() {
        if (threadPoolExecutor == null) {
            synchronized (ThreadPoolUtil.class) {
                if (threadPoolExecutor == null) {//双重锁,保证资源的合理利用

                    threadPoolExecutor = new ThreadPoolExecutor(4, 20, 5, TimeUnit.MINUTES, new LinkedBlockingQueue<>());
                }
            }

        }
        return threadPoolExecutor;
    }
}
