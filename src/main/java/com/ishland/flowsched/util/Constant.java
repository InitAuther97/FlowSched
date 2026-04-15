package com.ishland.flowsched.util;

import io.reactivex.rxjava3.core.Completable;

import java.util.concurrent.CancellationException;

public class Constant {
    public static final CancellationException CANCELLED = new CancellationException() {
        @Override
        public synchronized Throwable fillInStackTrace() {
            return this;
        }
    };

    public static final Completable FAILED_EMPTY_COMPLETABLE = Completable.error(CANCELLED);

    public static final Runnable NO_OP = () -> {
    };
}
