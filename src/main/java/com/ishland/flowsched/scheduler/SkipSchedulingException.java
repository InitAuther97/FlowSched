package com.ishland.flowsched.scheduler;

class SkipSchedulingException extends Exception {
    SkipSchedulingException(Throwable cause) {
        super(null, cause, true, false);
    }
}
