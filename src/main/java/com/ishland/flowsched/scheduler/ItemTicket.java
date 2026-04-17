package com.ishland.flowsched.scheduler;

import com.ishland.flowsched.util.Assertions;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public class ItemTicket {

    private static final VarHandle VH_CONSUMPTIONS;

    static {
        try {
            final var lookup = MethodHandles.lookup();
            VH_CONSUMPTIONS = lookup.findVarHandle(ItemTicket.class, "consumptions", int.class);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private final int hashCode;
    private final TicketType type;
    private final Object source;
    private Runnable callback;
    private volatile int consumptions;

    public ItemTicket(TicketType type, Object source, Runnable callback) {
        this(type, source, callback, 1);
    }

    public ItemTicket(TicketType type, Object source, Runnable callback, int consumptions) {
        this.type = Objects.requireNonNull(type);
        this.source = Objects.requireNonNull(source);
        this.callback = callback;
        this.consumptions = consumptions;
        Assertions.assertTrue(this.consumptions > 0);
        this.hashCode = this.hashCode0();
    }

    public Object getSource() {
        return this.source;
    }

    public TicketType getType() {
        return this.type;
    }

    public void consumeCallback() {
        int counter = (int) VH_CONSUMPTIONS.getAndAddRelease(this, -1);
        Assertions.assertTrue(counter > 0, "Counter underflow");
        if (counter > 1) {
            return;
        }
        VarHandle.acquireFence();
        final var callback = this.callback;
        this.callback = null;
        callback.run();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ItemTicket that = (ItemTicket) o;
        return type == that.type && Objects.equals(source, that.source);
    }

    private int hashCode0() {
        // inlined version of Objects.hash(type, source, targetStatus)
        int result = 1;

        result = 31 * result + type.hashCode();
        result = 31 * result + source.hashCode();
        return result;
    }

    @Override
    public int hashCode() {
        return this.hashCode;
    }

    public static class TicketType {
        public static TicketType DEPENDENCY = new TicketType("flowsched:dependency");
        public static TicketType EXTERNAL = new TicketType("flowsched:external");

        private final String description;

        public TicketType(String description) {
            this.description = description;
        }

        public String getDescription() {
            return this.description;
        }

        // use default equals() and hashCode()

    }
}
