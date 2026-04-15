package com.ishland.flowsched.scheduler;

import com.ishland.flowsched.util.Assertions;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public class Cancellable {

    protected Runnable onCancel;

    public void setup(Runnable onCancel) {
        final var result = VH_CANCEL.getAndSet(this, onCancel);
        Assertions.assertTrue(result != C_COMPLETED, "Cancellation is already completed when setup");
    }

    public boolean complete() {
        while (true) {
            final var witness = (Runnable) VH_CANCEL.getAcquire(this);
            if (witness == C_CANCELLED || witness == C_COMPLETED) {
                return false;
            }
            if (VH_CANCEL.weakCompareAndSetRelease(this, witness, C_COMPLETED)) {
                return true;
            }
        }
    }

    public boolean cancel() {
        while (true) {
            final Runnable witness = (Runnable) VH_CANCEL.get(this);
            if (witness == C_CANCELLED || witness == C_COMPLETED) {
                return false;
            }
            if (VH_CANCEL.weakCompareAndSetRelease(this, witness, C_CANCELLED)) {
                if (witness != null) {
                    VarHandle.acquireFence();
                    witness.run();
                }
                return true;
            }
        }
    }

    public boolean isCancelled() {
        return VH_CANCEL.getAcquire(this) == C_CANCELLED;
    }

    public boolean isCompleted() {
        return VH_CANCEL.getAcquire(this) == C_COMPLETED;
    }

    private static final VarHandle VH_CANCEL;
    private static final Runnable C_CANCELLED = () -> {};
    private static final Runnable C_COMPLETED = () -> {};

    public static final Cancellable COMPLETED = new Cancellable();
    public static final Cancellable CANCELLED = new Cancellable();

    static {
        try {
            VH_CANCEL = MethodHandles.lookup().findVarHandle(Cancellable.class, "onCancel", Runnable.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        COMPLETED.setup(C_COMPLETED);
        CANCELLED.setup(C_CANCELLED);
    }

}
