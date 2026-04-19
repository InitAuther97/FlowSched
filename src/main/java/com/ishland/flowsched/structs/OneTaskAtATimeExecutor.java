package com.ishland.flowsched.structs;

import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public class OneTaskAtATimeExecutor implements Executor, Runnable {

    private final AtomicBoolean currentlyRunning = new AtomicBoolean(false);
    private final Queue<Runnable> queue;
    private final Executor backingExecutor;

    public OneTaskAtATimeExecutor(Queue<Runnable> queue, Executor backingExecutor) {
        this.backingExecutor = backingExecutor;
        this.queue = queue;
    }

    @Override
    public void run() {
        try {
            Runnable command;
            while ((command = this.queue.poll()) != null) {
                try {
                    command.run();
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        } finally {
            // InitAuther97: volatile set: store cannot be reordered with queue.isEmpty check
            // set to false -> is empty -> the next offer will schedule
            // is empty -> the next offer skipped scheduling -> set to false => task leaking
            this.currentlyRunning.set(false);
            this.trySchedule();
        }
    }

    private void trySchedule() {
        if (!this.queue.isEmpty() && this.needsWakeup()) {
            this.backingExecutor.execute(this);
        }
    }

    private boolean needsWakeup() {
        return !this.currentlyRunning.getAndSet(true);
    }

    @Override
    public void execute(Runnable command) {
        this.queue.add(command);
        this.trySchedule();
    }
}
