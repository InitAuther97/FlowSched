package com.ishland.flowsched.structs;

import com.ishland.flowsched.executor.LockToken;
import com.ishland.flowsched.executor.Task;

import java.util.function.Consumer;

public class TestTask extends Task {
    @Override
    public void run(Consumer<? super Task> releaseLocks) {
        releaseLocks.accept(this);
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
