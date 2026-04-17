package com.ishland.flowsched.util;

import io.reactivex.rxjava3.core.Completable;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

public class Constant {
    public static final CancellationException CANCELLED = new CancellationException() {
        @Override
        public synchronized Throwable fillInStackTrace() {
            return this;
        }
    };

    public static final IllegalStateException UNLOADED_EXCEPTION = new IllegalStateException("Not loaded") {
        @Override
        public synchronized Throwable fillInStackTrace() {
            return this;
        }
    };

    public static final CompletableFuture<?> UNLOADED_FUTURE = CompletableFuture.failedFuture(UNLOADED_EXCEPTION);
    public static final CompletableFuture<?> COMPLETED_VOID_FUTURE = CompletableFuture.completedFuture(null);

    public static final Completable FAILED_EMPTY_COMPLETABLE = Completable.error(CANCELLED);

    public static final Runnable NO_OP = () -> {
    };
}
