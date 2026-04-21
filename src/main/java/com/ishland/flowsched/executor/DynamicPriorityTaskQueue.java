package com.ishland.flowsched.executor;

import it.unimi.dsi.fastutil.ints.IntReferencePair;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A priority queue with fixed number of priorities and allows changing priorities of elements.
 *
 * @param <E> the type of elements held in this collection
 */
public class DynamicPriorityTaskQueue<E extends Task> {

    private static final VarHandle VH_TASK_COUNT = MethodHandles.arrayElementVarHandle(int[].class);

    private final int queueId; // Owner tag
    private final int[] taskCount;
    private final ConcurrentLinkedQueue<E>[] priorities;

    public DynamicPriorityTaskQueue(int priorityCount) {
        queueId = QUEUE_ID.getAndIncrement();
        this.taskCount = new int[priorityCount];
        //noinspection unchecked
        this.priorities = new ConcurrentLinkedQueue[priorityCount];
        for (int i = 0; i < priorityCount; i++) {
            this.priorities[i] = new ConcurrentLinkedQueue<>();
        }
    }

    public void enqueue(E element, int priority) {
        if (priority < 0 || priority >= priorities.length)
            throw new IllegalArgumentException("Priority out of range");
        if (element.queueId > 0)
            throw new IllegalArgumentException("Task is already queued");

        VH_PRIORITY.set(element, priority);
        this.priorities[priority].add(element);
        VH_QUEUEID.setRelease(element, queueId);
        VH_TASK_COUNT.getAndAddAcquire(this.taskCount, priority, 1);
    }

    /**
     * Change the priority of a queued task. <br/>
     * @param element the task whose priority is to change
     * @param priority the target priority
     * @return The original priority if success, or one of the following if the operation fails:
     * {@link #R_REMOVED}, {@link #R_UNINITIALIZED}, {@link #R_FAILED}
     */
    public int changePriority(E element, int priority) {
        if (priority < 0 || priority >= priorities.length)
            throw new IllegalArgumentException("Priority out of range");

        // synchronize with enqueue
        int queueId = (int) VH_QUEUEID.getAcquire(element);
        if (queueId == Task.QID_NOT_YET_QUEUED) // The task may not be queued, or we are syncing with enqueue.
            return R_UNINITIALIZED; // Tolerate the case that we don't know it.
        if (queueId != this.queueId) // ?
            throw new IllegalArgumentException("Task is not in current queue. Witness=" + queueId + " != Queue=" + this.queueId);

        // priority is visible after syncing with enqueue
        // attempt to synchronize with the previous changePriority
        int currentPriority = (int) VH_PRIORITY.getAcquire(element);

        // 1) It is already polled. Clearly failure.
        // 2) It is already at the target priority. Nothing to do.
        if (currentPriority == priority || currentPriority < 0) {
            return currentPriority; // a clear failure
        }

        // We won't fail to execute a task because we're guaranteed to see the latest priority
        // when the one added here is polled
        int witness = (int) VH_PRIORITY.compareAndExchangeRelease(element, currentPriority, priority);
        if (witness != currentPriority) { // best-effort
            // something else may have removed it or dequeued it
            // or, something else changed its priority
            return witness >= 0 ? R_FAILED : witness;
        }
        VH_TASK_COUNT.getAndAddAcquire(this.taskCount, priority, 1);
        this.priorities[priority].add(element);
        return witness;
    }

    public IntReferencePair<E> dequeue() {
        for (int i = 0; i < this.priorities.length; i ++) {
            ConcurrentLinkedQueue<E> queue = this.priorities[i];
            E element = queue.poll();
            if (element == null) {
                continue;
            }
            VH_TASK_COUNT.getAndAddAcquire(this.taskCount, i, -1);
            return IntReferencePair.of(i, element);
        }
        return null;
    }

    public boolean contains(E element) {
        return queueId == (int) VH_QUEUEID.get(element);
    }

    /// This guarantees no memory order. Be careful.
    public void remove(E element) {
        // best-effort basis
        if (queueId != (int) VH_QUEUEID.getAcquire(element)) return; // not yet queued

        int witness;
        do {
            witness = (int) VH_PRIORITY.get(element);
            if (witness < 0) return; // removed or dequeued
        } while (!VH_PRIORITY.weakCompareAndSetPlain(element, witness, Task.P_REMOVED));

        VH_TASK_COUNT.getAndAddAcquire(this.taskCount, witness, -1);
    }

    public int size() {
        int estimate = 0;
        for (int i = 0; i < this.priorities.length; i++) {
            estimate += (int) VH_TASK_COUNT.get(this.taskCount, i);
        }
        return estimate;
    }



    // Results for changePriority. Used to increase responsiveness.

    /// Failed to change priority because the task is already dequeued.
    public static final int R_REMOVED = Task.P_REMOVED;
    /// Failed to change priority because the task is not yet queued,
    public static final int R_UNINITIALIZED = Task.P_UNINITIALIZED;
    /// Failed to change priority because it's changed by someone else.
    public static final int R_FAILED = -3;

    // Atomic access
    static final VarHandle VH_PRIORITY, VH_QUEUEID;
    // Owner tag allocator
    private static final AtomicInteger QUEUE_ID = new AtomicInteger(1);

    static {
        try {
            VH_QUEUEID = MethodHandles.lookup().findVarHandle(Task.class, "queueId", int.class);
            VH_PRIORITY = MethodHandles.lookup().findVarHandle(Task.class, "priority", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}


