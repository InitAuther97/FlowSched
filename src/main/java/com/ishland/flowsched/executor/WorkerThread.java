package com.ishland.flowsched.executor;

import it.unimi.dsi.fastutil.ints.IntReferencePair;

public class WorkerThread extends Thread {

    private final ExecutorManager executorManager;
    private volatile boolean shutdown = false;

    public WorkerThread(ExecutorManager executorManager) {
        this.executorManager = executorManager;
    }

    @Override
    public void run() {
        for (;;) {
            this.executorManager.waitObj.acquireUninterruptibly();

            while (!pollTasks()) {
                final boolean load = this.shutdown;
                if (load) return;
                Thread.onSpinWait();
            }
        }
    }

    private boolean pollTasks() {
        IntReferencePair<Task> pair = this.executorManager.getGlobalWorkQueue().dequeue();
        if (pair == null) {
            return false;
        }
        final int priority = pair.leftInt();
        final Task task = pair.right();
        if (priority == (int) DynamicPriorityTaskQueue.VH_PRIORITY.getAcquire(task) &&
                priority == (int) DynamicPriorityTaskQueue.VH_PRIORITY.compareAndExchangeAcquire(task, priority, Task.P_REMOVED) &&
                this.executorManager.tryLock(task))
            executorManager.runTask(task);

        return true; // polled
    }

    public void shutdown() {
        shutdown = true;
    }
}
