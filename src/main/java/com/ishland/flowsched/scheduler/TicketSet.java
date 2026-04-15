package com.ishland.flowsched.scheduler;

import com.ishland.flowsched.util.Assertions;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Set;

/**
 * Not thread-safe
 */
public class TicketSet<K, V, Ctx> {

    private static final VarHandle VH_TARGET_STATUS;

    static {
        try {
            VH_TARGET_STATUS = MethodHandles.lookup().findVarHandle(TicketSet.class, "targetStatus", int.class);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private final ItemStatus<K, V, Ctx> initialStatus;
    private final Set<ItemTicket>[] status2Tickets;
    private final int[] status2TicketsSize;
    private volatile int targetStatus;

    public TicketSet(ItemStatus<K, V, Ctx> initialStatus, ObjectFactory objectFactory) {
        this.initialStatus = initialStatus;
        this.targetStatus = initialStatus.getOrdinal();
        ItemStatus<K, V, Ctx>[] allStatuses = initialStatus.getAllStatuses();
        //noinspection unchecked
        this.status2Tickets = new Set[allStatuses.length];
        for (int i = 0; i < allStatuses.length; i++) {
            this.status2Tickets[i] = new ObjectOpenHashSet<>(ObjectOpenHashSet.DEFAULT_INITIAL_SIZE, ObjectOpenHashSet.FAST_LOAD_FACTOR);
        }
        this.status2TicketsSize = new int[allStatuses.length];
        // InitAuther97: no fullFence slop
        // VarHandle.fullFence();
    }

    public boolean checkAdd(ItemStatus<K, V, Ctx> targetStatus, ItemTicket ticket) {
        return this.status2Tickets[targetStatus.getOrdinal()].add(ticket);
    }

    public byte addUnchecked(ItemStatus<K, V, Ctx> targetStatus) {
        this.status2TicketsSize[targetStatus.getOrdinal()] ++;
        if (targetStatus.getOrdinal() > this.targetStatus) {
            // Pass this message to spin updaters
            return soTargetStatus(targetStatus.getOrdinal());
        }
        return lpTargetStatus();
    }

    public boolean checkRemove(ItemStatus<K, V, Ctx> targetStatus, ItemTicket ticket) {
        return this.status2Tickets[targetStatus.getOrdinal()].remove(ticket);
    }

    public byte removeUnchecked(ItemStatus<K, V, Ctx> targetStatus) {
        int updated = --this.status2TicketsSize[targetStatus.getOrdinal()];
        if (updated == 0) {
            // Pass this message to spin updaters
            return soTargetStatus(this.computeTargetStatusSlow());
        }
        return lpTargetStatus();
    }

    byte soTargetStatus(byte targetStatus) {
        VH_TARGET_STATUS.setRelease(this, targetStatus);
        return targetStatus;
    }

    byte lpTargetStatus() {
        return (byte) (int) VH_TARGET_STATUS.get(this);
    }

    byte loTargetStatus() {
        return (byte) (int) VH_TARGET_STATUS.getAcquire(this);
    }

    public ItemStatus<K, V, Ctx> getTargetStatus() {
        return this.initialStatus.getAllStatuses()[this.targetStatus];
    }

    public Set<ItemTicket> getTicketsForStatus(ItemStatus<K, V, Ctx> status) {
        return this.status2Tickets[status.getOrdinal()];
    }

    void clear() {
        for (Set<ItemTicket> tickets : status2Tickets) {
            tickets.clear();
        }

        // InitAuther97: no fullFence() slop
        // VarHandle.fullFence();
    }

    void assertEmpty() {
        for (Set<ItemTicket> tickets : status2Tickets) {
            Assertions.assertTrue(tickets.isEmpty());
        }
    }

    private byte computeTargetStatusSlow() {
        for (int i = this.status2Tickets.length - 1; i > 0; i--) {
            if (this.status2TicketsSize[i] > 0) {
                return (byte) i;
            }
        }
        return 0;
    }

}
