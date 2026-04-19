package com.ishland.flowsched.executor;

public class WorkerThread extends Thread {

    private final ExecutorManager executorManager;
    private volatile boolean shutdown = false;

    public WorkerThread(ExecutorManager executorManager) {
        this.executorManager = executorManager;
    }

    @Override
    public void run() {
        while (true) {
            this.executorManager.waitObj.acquireUninterruptibly();

            while (!pollTasks()) {
                final boolean load = this.shutdown;
                if (load) return;
                Thread.onSpinWait();
            }
        }
    }

    private boolean pollTasks() {
        Task task = this.executorManager.getGlobalWorkQueue().dequeue();
        if (task == null) {
            return false;
        }
        if (!this.executorManager.tryLock(task)) {
            return true; // polled
        }
        try {
            executorManager.runTask(task);
        } catch (Throwable _) {
        }
        return true;
    }

    public void shutdown() {
        shutdown = true;
    }
}
