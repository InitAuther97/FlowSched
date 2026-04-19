package com.ishland.flowsched.executor;

import java.util.Objects;
import java.util.function.Consumer;

public class SimpleTask extends Task {

    private final Runnable wrapped;

    public SimpleTask(Runnable wrapped) {
        this.wrapped = Objects.requireNonNull(wrapped);
    }

    @Override
    public void run(Consumer<? super Task> releaseLocks) {
        try {
            wrapped.run();
        } finally {
            releaseLocks.accept(this);
        }
    }

    @Override
    public void propagateException(Throwable t) {
        t.printStackTrace();
    }

    @Override
    public LockToken[] lockTokens() {
        return new LockToken[0];
    }
}
