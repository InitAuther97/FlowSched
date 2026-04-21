package com.ishland.flowsched.executor;

import com.ishland.flowsched.util.Assertions;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;

public class ExecutorManager {

    private static final VarHandle VH_LOCK;
    private static final Logger LOGGER = LoggerFactory.getLogger("FlowSched ExecutorManager");

    static {
        try {
            VH_LOCK = MethodHandles.lookup().findVarHandle(Task.class, "heldLock", int.class);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private final DynamicPriorityTaskQueue<Task> globalWorkQueue;
    private final ConcurrentMap<LockToken, FreeableTaskList> lockListeners = new ConcurrentHashMap<>();
    private final WorkerThread[] workerThreads;
    private final Consumer<? super Task> releaseLocks = this::doReleaseLock;
    public final Semaphore waitObj = new Semaphore(0);

    /**
     * Creates a new executor manager.
     *
     * @param workerThreadCount the number of worker threads.
     */
    public ExecutorManager(int workerThreadCount) {
        this(workerThreadCount, thread -> {});
    }

    /**
     * Creates a new executor manager.
     *
     * @param workerThreadCount the number of worker threads.
     * @param threadInitializer the thread initializer.
     */
    public ExecutorManager(int workerThreadCount, Consumer<Thread> threadInitializer) {
        this(workerThreadCount, threadInitializer, 64);
    }

    /**
     * Creates a new executor manager.
     *
     * @param workerThreadCount the number of worker threads.
     * @param threadInitializer the thread initializer.
     * @param priorityCount the number of priorities.
     */
    public ExecutorManager(int workerThreadCount, Consumer<Thread> threadInitializer, int priorityCount) {
        globalWorkQueue = new DynamicPriorityTaskQueue<>(priorityCount);
        workerThreads = new WorkerThread[workerThreadCount];
        // InitAuther97: make sure work queue is fully initialized before workers are started
        VarHandle.fullFence();
        for (int i = 0; i < workerThreadCount; i++) {
            final WorkerThread thread = new WorkerThread(this);
            threadInitializer.accept(thread);
            workerThreads[i] = thread;
            thread.start();
        }
    }

    /**
     * Attempt to lock the given tokens.
     * The caller should discard the task if this method returns false, as it reschedules the task.
     *
     * @return {@code true} if the lock is acquired, {@code false} otherwise.
     */
    boolean tryLock(Task task) {
        retry:
        while (true) {
            final FreeableTaskList listenerSet = new FreeableTaskList();
            LockToken[] lockTokens = task.lockTokens();
            for (int i = 0; i < lockTokens.length; i++) {
                LockToken token = lockTokens[i];
                final FreeableTaskList present = this.lockListeners.putIfAbsent(token, listenerSet);
                if (present != null) {
                    for (int j = 0; j < i; j++) {
                        Assertions.assertTrue(this.lockListeners.remove(lockTokens[j], listenerSet));
                    }
                    callListeners(listenerSet); // synchronizes
                    synchronized (present) {
                        if (present.freed) {
                            continue retry;
                        } else {
                            present.add(task);
                        }
                    }
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Release the locks held by the given task.
     * @param task the task.
     */
    void releaseLocks(Task task) {
        FreeableTaskList expectedListeners = null;
        for (LockToken token : task.lockTokens()) {
            final FreeableTaskList listeners = this.lockListeners.remove(token);
            if (listeners != null) {
                if (expectedListeners == null) {
                    expectedListeners = listeners;
                } else {
                    Assertions.assertTrue(expectedListeners == listeners, "Inconsistent lock listeners");
                }
            } else {
                throw new IllegalStateException("Lock token " + token + " is not locked");
            }
        }
        if (expectedListeners != null) {
            callListeners(expectedListeners); // synchronizes
        }
    }

    private void callListeners(FreeableTaskList listeners) {
        synchronized (listeners) {
            listeners.freed = true;
        }
        if (listeners.isEmpty()) return;
        for (Task listener : listeners) {
            listener.reset();
            this.schedule0(listener, listener.pendingPriority);
        }
    }

    void runTask(Task task) {
        try {
            task.run(releaseLocks);
        } catch (Throwable t) {
            LOGGER.error("Exception thrown while executing task", t);
            try {
                doReleaseLock(task);
            } catch (Throwable t1) {
                t.addSuppressed(t1);
                LOGGER.error("Exception thrown while releasing locks", t);
            }
            try {
                task.propagateException(t);
            } catch (Throwable t1) {
                t.addSuppressed(t1);
                LOGGER.error("Exception thrown while propagating exception", t);
            }
        }
    }

    void doReleaseLock(Task task) {
        if (1 == (int) VH_LOCK.getAndSetAcquire(task, 0)) {
            this.releaseLocks(task);
        }
    }

    DynamicPriorityTaskQueue<Task> getGlobalWorkQueue() {
        return this.globalWorkQueue;
    }

    /**
     * Shuts down the executor manager.
     */
    public void shutdown() {
        for (WorkerThread workerThread : workerThreads) {
            workerThread.shutdown();
        }
        this.waitObj.release(workerThreads.length * 128);
    }

    /**
     * Schedules a task.
     * @param task the task.
     */
    public void schedule(Task task, int priority) {
        schedule0(task, priority);
    }

    private void schedule0(Task task, int priority) {
        task.pendingPriority = priority;
        this.globalWorkQueue.enqueue(task, priority);
        this.waitObj.release(1);
    }

    /**
     * Schedules a runnable for execution with the given priority.
     *
     * @param runnable the runnable.
     * @param priority the priority.
     */
    public void schedule(Runnable runnable, int priority) {
        this.schedule(new SimpleTask(runnable), priority);
    }

    /**
     * Creates an executor that schedules runnables with the given priority.
     *
     * @param priority the priority.
     * @return the executor.
     */
    public Executor executor(int priority) {
        return runnable -> this.schedule(runnable, priority);
    }

    /**
     * Notifies the executor manager that the priority of the given task has changed.
     *
     * @param task the task.
     */
    public void changePriority(Task task, int priority) {
        task.pendingPriority = priority;
        int result = this.globalWorkQueue.changePriority(task, priority);
        while (result == DynamicPriorityTaskQueue.R_UNINITIALIZED) {
            Thread.onSpinWait();
            result = this.globalWorkQueue.changePriority(task, priority);
        }
        if (result > 0 && result != priority) {
            this.waitObj.release(1);
        }
    }

    private static class FreeableTaskList extends ReferenceArrayList<Task> {

        private boolean freed = false;

        @Override
        public boolean equals(Object o) {
            // InitAuther97: force identity comparison
            // Maps may use equals() to compare value when removing a given K-V pair
            // It's our intention to use identity comparison, force it here!
            return this == o;
        }

        @Override
        public int hashCode() {
            // InitAuther97: force identity comparison
            return System.identityHashCode(this);
        }
    }

}
