/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.common;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public abstract class ServiceThread implements Runnable {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private static final long JOIN_TIME = 90 * 1000;

    private Thread thread;
    // waitPoint 起到主线程通知子线程的作用
    protected final CountDownLatch2 waitPoint = new CountDownLatch2(1);
    // 是通知标识
    protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);
    // 停止标识
    protected volatile boolean stopped = false;
    // 是否守护线程
    protected boolean isDaemon = false;

    // 线程开始标识
    private final AtomicBoolean started = new AtomicBoolean(false);

    public ServiceThread() {

    }

    // 获取线程名称
    public abstract String getServiceName();

    // 开始执行任务
    public void start() {
        log.info("Try to start service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        // 任务已经开始运行标识
        if (!started.compareAndSet(false, true)) {
            return;
        }
        // 停止标识设置为 false
        stopped = false;
        // 绑定线程，运行当前任务
        this.thread = new Thread(this, getServiceName());
        // 设置守护线程，守护线程具有最低的优先级，一般用于为系统中的其它对象和线程提供服务
        this.thread.setDaemon(isDaemon);
        // 启动线程开始运行
        this.thread.start();
    }

    public void shutdown() {
        this.shutdown(false);
    }

    public void shutdown(final boolean interrupt) {
        log.info("Try to shutdown service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        // 任务必须已经开始
        if (!started.compareAndSet(true, false)) {
            return;
        }
        // 设置停止标识
        this.stopped = true;
        log.info("shutdown thread " + this.getServiceName() + " interrupt " + interrupt);

        if (hasNotified.compareAndSet(false, true)) {
            // 计数减1，通知等待的线程不要等待了
            waitPoint.countDown(); // notify
        }

        try {
            // 中断线程，设置中断标识
            if (interrupt) {
                this.thread.interrupt();
            }

            // 守护线程等待执行完毕
            long beginTime = System.currentTimeMillis();
            if (!this.thread.isDaemon()) {
                this.thread.join(this.getJointime());
            }
            long elapsedTime = System.currentTimeMillis() - beginTime;
            log.info("join thread " + this.getServiceName() + " elapsed time(ms) " + elapsedTime + " "
                + this.getJointime());
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }
    }

    public long getJointime() {
        return JOIN_TIME;
    }

    @Deprecated
    public void stop() {
        this.stop(false);
    }

    @Deprecated
    public void stop(final boolean interrupt) {
        if (!started.get()) {
            return;
        }
        this.stopped = true;
        log.info("stop thread " + this.getServiceName() + " interrupt " + interrupt);

        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }

        if (interrupt) {
            this.thread.interrupt();
        }
    }

    public void makeStop() {
        if (!started.get()) {
            return;
        }
        this.stopped = true;
        log.info("makestop thread " + this.getServiceName());
    }

    public void wakeup() {
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }
    }

    // 等待一定时间后运行
    protected void waitForRunning(long interval) {
        if (hasNotified.compareAndSet(true, false)) {
            // 通知等待结束
            this.onWaitEnd();
            return;
        }

        // 重置计数
        waitPoint.reset();

        try {
            // 一直等待，直到计数减为 0，或者超时
            waitPoint.await(interval, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        } finally {
            // 设置未通知
            hasNotified.set(false);
            // 通知等待结束
            this.onWaitEnd();
        }
    }

    protected void onWaitEnd() {
    }

    public boolean isStopped() {
        return stopped;
    }

    public boolean isDaemon() {
        return isDaemon;
    }

    public void setDaemon(boolean daemon) {
        isDaemon = daemon;
    }
}
