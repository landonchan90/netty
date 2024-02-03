/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.util.concurrent;

import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.ThreadExecutorMap;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import org.jetbrains.annotations.Async.Schedule;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Single-thread singleton {@link EventExecutor}.  It starts the thread automatically and stops it when there is no
 * task pending in the task queue for {@code io.netty.globalEventExecutor.quietPeriodSeconds} second
 * (default is 1 second).  Please note it is not scalable to schedule large number of tasks to this executor;
 * use a dedicated executor.
 */

//当任务队列中没有待处理的任务，1秒钟后，它将关闭线程，有新的任务加入，线程重新创建并启动
public final class GlobalEventExecutor extends AbstractScheduledEventExecutor implements OrderedEventExecutor {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(GlobalEventExecutor.class);

    private static final long SCHEDULE_QUIET_PERIOD_INTERVAL;

    static {
        //io.netty.globalEventExecutor.quietPeriodSeconds或者1S为空闲间隔.
        int quietPeriod = SystemPropertyUtil.getInt("io.netty.globalEventExecutor.quietPeriodSeconds", 1);
        if (quietPeriod <= 0) {
            quietPeriod = 1;
        }
        logger.debug("-Dio.netty.globalEventExecutor.quietPeriodSeconds: {}", quietPeriod);

        SCHEDULE_QUIET_PERIOD_INTERVAL = TimeUnit.SECONDS.toNanos(quietPeriod);
    }

    public static final GlobalEventExecutor INSTANCE = new GlobalEventExecutor();

    final BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<Runnable>();

    //一个任务，第一个参数表示1秒后到执行时间，第二个参数表示每次执行间隔1秒。
    final ScheduledFutureTask<Void> quietPeriodTask = new ScheduledFutureTask<Void>(
            this, Executors.<Void>callable(new Runnable() {
        @Override
        public void run() {
            // NOOP
        }
    }, null),
            // note: the getCurrentTimeNanos() call here only works because this is a final class, otherwise the method
            // could be overridden leading to unsafe initialization here!
            deadlineNanos(getCurrentTimeNanos(), SCHEDULE_QUIET_PERIOD_INTERVAL),
            -SCHEDULE_QUIET_PERIOD_INTERVAL
    );

    // because the GlobalEventExecutor is a singleton, tasks submitted to it can come from arbitrary threads and this
    // can trigger the creation of a thread from arbitrary thread groups; for this reason, the thread factory must not
    // be sticky about its thread group
    // visible for testing
    final ThreadFactory threadFactory;
    private final TaskRunner taskRunner = new TaskRunner();
    private final AtomicBoolean started = new AtomicBoolean();
    volatile Thread thread;

    private final Future<?> terminationFuture = new FailedFuture<Object>(this, new UnsupportedOperationException());

    private GlobalEventExecutor() {
        //构造参数把quietPeriodTask加入到底层队列，底层队列是有顺序的
        //根据quietPeriodTask它判断底层队列是否还有新的任务
        //这里调用了scheduledTaskQueue()方法，底层已经创建了scheduledTaskQueue
        //并把noop标记作用的队列加入到父类排序队列，这个任务永远不会删除，每次运行后1秒以后再次运行。
        //它的作用是排序，它出现了说明系统过了1秒，可以结合队列的数量判断队列1秒过后是否还有任务。
        scheduledTaskQueue().add(quietPeriodTask);
        threadFactory = ThreadExecutorMap.apply(new DefaultThreadFactory(
                DefaultThreadFactory.toPoolName(getClass()), false, Thread.NORM_PRIORITY, null), this);
    }

    /**
     * Take the next {@link Runnable} from the task queue and so will block if no task is currently present.
     *
     * @return {@code null} if the executor thread has been interrupted or waken up.
     */
    Runnable takeTask() {
        BlockingQueue<Runnable> taskQueue = this.taskQueue;
        for (;;) {
            //父类排序度列，取出头元素但不移除元素
            ScheduledFutureTask<?> scheduledTask = peekScheduledTask();
            //初始化时已经加了quitetask.
            if (scheduledTask == null) {
                Runnable task = null;
                try {
                    task = taskQueue.take();
                } catch (InterruptedException e) {
                    // Ignore
                }
                return task;
            } else {
                //如果delayNanos>0说明没到执行时间
                long delayNanos = scheduledTask.delayNanos();
                Runnable task = null;
                if (delayNanos > 0) {
                    try {
                        //去任务队列里面去拿任务
                        task = taskQueue.poll(delayNanos, TimeUnit.NANOSECONDS);
                    } catch (InterruptedException e) {
                        // Waken up.
                        return null;
                    }
                }
                if (task == null) {
                    // We need to fetch the scheduled tasks now as otherwise there may be a chance that
                    // scheduled tasks are never executed if there is always one task in the taskQueue.
                    // This is for example true for the read task of OIO Transport
                    // See https://github.com/netty/netty/issues/1614
                    fetchFromScheduledTaskQueue();
                    task = taskQueue.poll();
                }

                if (task != null) {
                    return task;
                }
            }
        }
    }

    //把quitetask放到任务队列里面，其实就是不断的等待新的任务进来
    private void fetchFromScheduledTaskQueue() {
        long nanoTime = getCurrentTimeNanos();
        Runnable scheduledTask = pollScheduledTask(nanoTime);
        while (scheduledTask != null) {
            taskQueue.add(scheduledTask);
            scheduledTask = pollScheduledTask(nanoTime);
        }
    }

    /**
     * Return the number of tasks that are pending for processing.
     */
    public int pendingTasks() {
        return taskQueue.size();
    }

    /**
     * Add a task to the task queue, or throws a {@link RejectedExecutionException} if this instance was shutdown
     * before.
     */
    private void addTask(Runnable task) {
        taskQueue.add(ObjectUtil.checkNotNull(task, "task"));
    }

    @Override
    public boolean inEventLoop(Thread thread) {
        return thread == this.thread;
    }

    @Override
    public Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit) {
        return terminationFuture();
    }

    @Override
    public Future<?> terminationFuture() {
        return terminationFuture;
    }

    @Override
    @Deprecated
    public void shutdown() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isShuttingDown() {
        return false;
    }

    @Override
    public boolean isShutdown() {
        return false;
    }

    @Override
    public boolean isTerminated() {
        return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
        return false;
    }

    /**
     * Waits until the worker thread of this executor has no tasks left in its task queue and terminates itself.
     * Because a new worker thread will be started again when a new task is submitted, this operation is only useful
     * when you want to ensure that the worker thread is terminated <strong>after</strong> your application is shut
     * down and there's no chance of submitting a new task afterwards.
     *
     * @return {@code true} if and only if the worker thread has been terminated
     */
    public boolean awaitInactivity(long timeout, TimeUnit unit) throws InterruptedException {
        ObjectUtil.checkNotNull(unit, "unit");

        final Thread thread = this.thread;
        if (thread == null) {
            throw new IllegalStateException("thread was not started");
        }
        thread.join(unit.toMillis(timeout));
        return !thread.isAlive();
    }

    @Override
    public void execute(Runnable task) {
        execute0(task);
    }

    private void execute0(@Schedule Runnable task) {
        //执行任务，加入队列，如果当前执行器没启动则启动
        addTask(ObjectUtil.checkNotNull(task, "task"));
        if (!inEventLoop()) {
            startThread();
        }
    }

    private void startThread() {
        //原子判断线程是否启动,返回true说明之前没启动
        if (started.compareAndSet(false, true)) {
            final Thread t = threadFactory.newThread(taskRunner);
            // Set to null to ensure we not create classloader leaks by holds a strong reference to the inherited
            // classloader.
            // See:
            // - https://github.com/netty/netty/issues/7290
            // - https://bugs.openjdk.java.net/browse/JDK-7008595
            AccessController.doPrivileged(new PrivilegedAction<Void>() {
                @Override
                public Void run() {
                    t.setContextClassLoader(null);
                    return null;
                }
            });

            // Set the thread before starting it as otherwise inEventLoop() may return false and so produce
            // an assert error.
            // See https://github.com/netty/netty/issues/4357
            thread = t;
            t.start();
        }
    }

    final class TaskRunner implements Runnable {
        @Override
        public void run() {
            for (;;) {
                Runnable task = takeTask();
                if (task != null) {
                    try {
                        runTask(task);
                    } catch (Throwable t) {
                        logger.warn("Unexpected exception from the global event executor: ", t);
                    }
                    //如果Task不是quietPeriodTask对象，则继续循环
                    //如果是quietPeriodTask对象，说明1秒时间过了没有其它任务，需要判断队列是否已经空了，如果空了则结束线程。
                    if (task != quietPeriodTask) {
                        continue;
                    }
                }

                Queue<ScheduledFutureTask<?>> scheduledTaskQueue = GlobalEventExecutor.this.scheduledTaskQueue;
                // Terminate if there is no task in the queue (except the noop task).
                //如果类队列为空，父类队列只有noop-task(quietPeriodTask),说明已经没用后续任务了
                if (taskQueue.isEmpty() && (scheduledTaskQueue == null || scheduledTaskQueue.size() == 1)) {
                    // Mark the current thread as stopped.
                    // The following CAS must always success and must be uncontended,
                    // because only one thread should be running at the same time.
                    boolean stopped = started.compareAndSet(true, false);
                    assert stopped;

                    // Check if there are pending entries added by execute() or schedule*() while we do CAS above.
                    // Do not check scheduledTaskQueue because it is not thread-safe and can only be mutated from a
                    // TaskRunner actively running tasks.
                    //如果在此时刻，其它线程调用了execute方法，那么队列数量会增加,就不会进入下面方法。
                    if (taskQueue.isEmpty()) {
                        // A) No new task was added and thus there's nothing to handle
                        //    -> safe to terminate because there's nothing left to do
                        // B) A new thread started and handled all the new tasks.
                        //    -> safe to terminate the new thread will take care the rest
                        break;
                    }

                    // There are pending tasks added again.
                    //如果在此时刻，其它线程调用了execute方法，那么队列数量会增加,cas失败结束
                    if (!started.compareAndSet(false, true)) {
                        // startThread() started a new thread and set 'started' to true.
                        // -> terminate this thread so that the new thread reads from taskQueue exclusively.
                        break;
                    }

                    // New tasks were added, but this worker was faster to set 'started' to true.
                    // i.e. a new worker thread was not started by startThread().
                    // -> keep this thread alive to handle the newly added entries.
                }
            }
        }
    }
}
