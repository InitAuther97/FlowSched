package com.ishland.flowsched.executor;

import java.util.function.Consumer;

public abstract class Task {

    public static final int P_UNINITIALIZED = -2, P_REMOVED = -1, QID_NOT_YET_QUEUED = -1;

    int queueId = QID_NOT_YET_QUEUED;
    int priority = P_UNINITIALIZED;
    int pendingPriority = P_UNINITIALIZED;
    int heldLock = 1;

    public abstract void run(Consumer<? super Task> releaseLocks);

    public abstract void propagateException(Throwable t);

    public abstract LockToken[] lockTokens();

    void reset() {
        queueId = QID_NOT_YET_QUEUED;
        priority = P_UNINITIALIZED;
    }
}

