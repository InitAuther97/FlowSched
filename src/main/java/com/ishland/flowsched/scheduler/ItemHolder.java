package com.ishland.flowsched.scheduler;

import com.ishland.flowsched.structs.OneTaskAtATimeExecutor;
import com.ishland.flowsched.util.Assertions;
import io.reactivex.rxjava3.core.Completable;
import it.unimi.dsi.fastutil.objects.*;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

import static com.ishland.flowsched.util.Constant.*;

@SuppressWarnings("unused")
class ItemHolderHotField {
    // private long l0, l1, l2, l3, l4, l5, l6, l7;
    /// flag_free (1bit) | flag_dirty (1bit) | flag_broken (1bit) | flag_removed (1bit) | changing status (5bit) | status (5bit) | ticket bitset (32bit)
    protected volatile long state; // Core synchronization point, responsible for upgrade/downgrade/future
    /// flag_flush (1bit)
    protected volatile int schedulerState;
}

class ItemHolderPadding1 extends ItemHolderHotField {
    private int i1;
    private long l12, l13, l14, l15, l16, l17; // padding
}

public class ItemHolder<K, V, Ctx, UserData> extends ItemHolderPadding1 {

    // private static final VarHandle VH_SCHEDULED_DIRTY;
    static final VarHandle VH_STATE, VH_SCHEDULER_STATE;

    public static final long FLAG_REMOVED = 1L << 42;
    /**
     * Indicates the holder have been marked broken
     * If set, the holder:
     * - will not be allowed to be upgraded any further
     * - will still be allowed to be downgraded, but operations to it should be careful
     */
    public static final long FLAG_BROKEN = 1L << 43;

    public static final long FLAG_DIRTY = 1L << 44;

    public static final long FLAG_FREE = 1L << 45;

    static {
        try {
            final var lookup = MethodHandles.lookup();
            // VH_SCHEDULED_DIRTY = lookup.findVarHandle(ItemHolder.class, "scheduledDirty", int.class);
            VH_STATE = lookup.findVarHandle(ItemHolder.class, "state", long.class);
            VH_SCHEDULER_STATE = lookup.findVarHandle(ItemHolder.class, "schedulerState", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private final K key;
    private UserData userData; // Stable value
    private final ItemStatus<K, V, Ctx> unloadedStatus;
    private final OneTaskAtATimeExecutor criticalSectionExecutor;

//  private final List<Pair<ItemStatus<K, V, Ctx>, Long>> statusHistory = ReferenceLists.synchronize(new ReferenceArrayList<>());
    private final KeyStatusPair<K, V, Ctx>[][] requestedDependencies;
    private final Object2ReferenceLinkedOpenHashMap<K, int[]> dependencyRefCnts = new Object2ReferenceLinkedOpenHashMap<>() {
        @Override
        protected void rehash(int newN) {
            if (n < newN) {
                super.rehash(newN);
            }
        }
    };
    private final Object2ReferenceFunction<K, int[]> depRefCntCreate;
    private boolean dependencyDirty = false; // Used in dependency critical section

    private final Set<ItemTicket>[] tickets;
    private final CompletableFuture<?>[] futures; // Futures to fire by setStatus, only written by ticket ops threads
    private V item; // Piggyback on future when read off scheduler threads
    private Cancellable runningAction = null; // Only used by scheduler threads

    ItemHolder(ItemStatus<K, V, Ctx> initialStatus, K key, ObjectFactory objectFactory, Executor backgroundExecutor) {
        this.unloadedStatus = Objects.requireNonNull(initialStatus);
        this.key = Objects.requireNonNull(key);
        ItemStatus<K, V, Ctx>[] allStatuses = initialStatus.getAllStatuses();
        this.tickets = new Set[allStatuses.length];
        for (int i = 0; i < allStatuses.length; i++) {
            this.tickets[i] = new ObjectOpenHashSet<>(ObjectOpenHashSet.DEFAULT_INITIAL_SIZE, ObjectOpenHashSet.FAST_LOAD_FACTOR);
        }
        this.futures = new CompletableFuture[allStatuses.length];
        this.requestedDependencies = new KeyStatusPair[allStatuses.length][];
        for (int i = 0, allStatusesLength = allStatuses.length; i < allStatusesLength; i++) {
            this.futures[i] = UNLOADED_FUTURE;
            this.requestedDependencies[i] = null;
        }
        this.criticalSectionExecutor = new OneTaskAtATimeExecutor(new ConcurrentLinkedQueue<>(), backgroundExecutor);
        final int length = initialStatus.getAllStatuses().length;
        this.depRefCntCreate = k -> {
            int[] refCnt = new int[length];
            Arrays.fill(refCnt, -1);
            return refCnt;
        };
        VH_STATE.set(this, 1 | FLAG_FREE);
        // InitAuther97: no fullFence slop
        // VarHandle.fullFence();
    }

    static byte getTargetStatus(long state) {
        Assertions.assertTrue((state & 1) != 0);
        return (byte) (31 - Integer.numberOfLeadingZeros((int) state));
    }

    static byte getStatus(long state) {
        return (byte) ((state >>> ItemStatus.STATUS_LENGTH) & ItemStatus.STATUS_MASK);
    }

    static long withStatus(long state, byte status) {
        return state & ~((long) ItemStatus.STATUS_MASK << ItemStatus.STATUS_LENGTH)
                | (long) status << ItemStatus.STATUS_LENGTH;
    }

    static byte getNextStatus(long state) {
        return (byte) ((state >>> (ItemStatus.STATUS_SIZE + ItemStatus.STATUS_LENGTH)) & ItemStatus.STATUS_MASK);
    }

    static long withNextStatus(long state, byte nextStatus) {
        return (state & ~((long) ItemStatus.STATUS_MASK << (ItemStatus.STATUS_SIZE + ItemStatus.STATUS_LENGTH)))
                | ((long) nextStatus << (ItemStatus.STATUS_SIZE + ItemStatus.STATUS_LENGTH));
    }

    boolean casRelStatus(long expected, byte status) {
        return VH_STATE.weakCompareAndSetRelease(this, expected, withStatus(expected, status));
    }

    boolean casStateRelaxed(long expected, long next) {
        return expected == (long) VH_STATE.compareAndExchangeAcquire(this, expected, next);
    }

    long andStateRelaxed(long and) {
        return (long) VH_STATE.getAndBitwiseAndAcquire(this, and);
    }

    long setTargetRelease(byte ordinal) {
        return (long) VH_STATE.getAndBitwiseOrRelease(this, 1L << ordinal);
    }

    long unsetTargetRelease(byte ordinal) {
        return (long) VH_STATE.getAndBitwiseAndRelease(this, ~(1L << ordinal));
    }

    /// Load the state with the acquire semantics. Useful when determining
    /// the state of the holder from external.
    long loState() {
        return (long) VH_STATE.getAcquire(this);
    }

    /// Load the state, allowing possible register hoisting to happen.
    /// This is usually okay when the value is later verified using CAS or
    /// is guaranteed to be valid by implicit memory ordering from the context.
    long lpState() {
        return (long) VH_STATE.get(this);
    }

    /**
     * Not thread-safe, protect with statusMutex
     */
    private void createFutures(byte from, byte to, byte status) {
        for (int i = from + 1; i <= to; i++) {
            if (this.futures[i] != UNLOADED_FUTURE) {
                failCreateFutures(this.futures[i]);
            }
            final var future = i <= status ? COMPLETED_VOID_FUTURE : new CompletableFuture<>();
            // InitAuther97: CompletableFuture does nothing in its constructor,
            // therefore it is guaranteed by JVM to be well initialized when shared
            // VarHandle.storeStoreFence(); // ensure visibility
            this.futures[i] = future;
        }
    }

    private static void failCreateFutures(CompletableFuture<?> found) {
        throw new IllegalStateException("Futures are in an incorrect state: not UNLOADED after target status, found " + found);
    }

    /**
     * Get the target status of this item.
     * The result expires immediately without proper synchronization.
     * @return the target status of this item, or null if no ticket is present
     */
    public ItemStatus<K, V, Ctx> getTargetStatus() {
        /*synchronized (this.tickets) {
            return this.tickets.getTargetStatus();
        }*/
        return this.unloadedStatus.getAt(getTargetStatus(loState()));
    }

    boolean tryLockSchedulerRelaxed() {
        long state = (long) VH_STATE.get(this);
        if ((state & FLAG_FREE) == 0) return false;
        return casStateRelaxed(state, state & ~FLAG_FREE);
    }

    void rescheduleTick(StatusAdvancingScheduler<K, V, Ctx, UserData> scheduler, boolean skipScheduling) {
        long state = setFlag(skipScheduling ? FLAG_FREE : FLAG_FREE | FLAG_DIRTY);
        if (!skipScheduling && (state & FLAG_DIRTY) == 0) {
            scheduleTick(scheduler);
        }
    }

    ItemHolder<K, V, Ctx, UserData>[] allUnmetDeps(StatusAdvancingScheduler<K, V, Ctx, UserData> scheduler, int ordinal, int level) {
        final var array = this.requestedDependencies[ordinal];
        if (array == null) return new ItemHolder[0];
        final var result = new ItemHolder[array.length];
        for (int i = 0; i < array.length; i++) {
            final var holder = scheduler.getHolder(array[i].key());
            if (holder.getStatus().getOrdinal() >= level) continue;
            result[i] = holder;
        }
        return result;
    }

    /*
    private boolean rescueHolder(ItemStatus<K, V, Ctx> targetStatus, long state) {
        final byte target = targetStatus.getOrdinal();
        if ((state & FLAG_REMOVED) != 0) return false;
        return 0L == (FLAG_REMOVED & orRelState(1L << target));
    }
     */

    public boolean addTicket(ItemStatus<K, V, Ctx> targetStatus, ItemTicket ticket) {
        Objects.requireNonNull(ticket);
        long state;
        /*
        final boolean hasRescued;
        if ((state & FLAG_REMOVED) == 0 && getTargetStatus(state) == unloadedOrdinal) {
            hasRescued = true;
            if (!rescueHolder(targetStatus, state)) return false;
        } else {
            hasRescued = false;
        }
         */
        final byte ordinal = targetStatus.getOrdinal();
        block:
        synchronized (this) {
            state = lpState();
            if ((state & FLAG_REMOVED) != 0) {
                return false;
            }
            final var set = this.tickets[ordinal];
            final boolean change = set.isEmpty();
            if (!set.add(ticket)) {
                throw new IllegalStateException("Ticket already exists");
            }
            if (!change) {
                break block;
            }
            state = setTargetRelease(ordinal);
            if ((state & FLAG_REMOVED) != 0) {
                return false;
            }
            final byte oldTarget = getTargetStatus(state);
            final byte status = getStatus(state);
            Assertions.assertTrue(oldTarget != -1);
            if (ordinal > oldTarget) {
                createFutures(oldTarget, ordinal, status);
            }
        }
        byte target = targetStatus.getOrdinal();
        final byte current = getStatus(state);
        if (current >= target) {
            ticket.consumeCallback();
        }
        return true;
    }

    public void removeTicket(ItemStatus<K, V, Ctx> targetStatus, ItemTicket ticket) {
        assertOpen();
        CompletableFuture<?>[] futuresToFail;
        final byte ordinal = targetStatus.getOrdinal();
        synchronized (this) {
            final var set = this.tickets[ordinal];
            if (!set.remove(ticket)) {
                throw new IllegalStateException("Ticket does not exist");
            }
            if (!set.isEmpty()) {
                return;
            }

            final long mask = ~(1L << ordinal);
            // InitAuther97: the affected futures are all masked by getFutureForStatus0 to UNLOADED_FUTURE.
            // if they unfortunately modify the futures (inserting a new one), it will be completed exceptionally later.
            // Release semantics is used here to support the use of state check as a synchronization point
            final long oldState = lpState();
            final byte oldTarget = getTargetStatus(oldState), newTarget = getTargetStatus(oldState & mask);

            if (oldTarget == newTarget) {
                unsetTargetRelease(ordinal);
                return;
            }

            futuresToFail = new CompletableFuture[oldTarget - newTarget];
            for (int i = newTarget + 1; i <= oldTarget; i++) {
                futuresToFail[i - newTarget - 1] = this.futures[i];
                this.futures[i] = UNLOADED_FUTURE;
            }
            unsetTargetRelease(ordinal);
        }

        // InitAuther97: now we fail any future that either exists before removing
        // noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < futuresToFail.length; i++) {
            futuresToFail[i].completeExceptionally(UNLOADED_EXCEPTION);
        }
    }

    public void subscribeOp(Completable op, StatusAdvancingScheduler<K, V, Ctx, UserData> scheduler) {
        assertOpen();
        op.subscribe(
                () -> rescheduleTick(scheduler, false),
                t -> rescheduleTick(scheduler, t instanceof SkipSchedulingException)
        );
    }

    // sync externally
    public void finishAction() {
        assertOpen();
        Assertions.assertTrue(this.runningAction != null, "No action is present when trying to finish an action");
        this.runningAction = null;
    }

    // sync externally
    public void submitAction(Cancellable cancellation) {
        assertOpen();
        Assertions.assertTrue(this.runningAction == null, "Only one action can happen at a time");
        this.runningAction = cancellation;
    }

    // sync externally
    public void tryCancelAction() {
        assertOpen();
        final Cancellable signaller = this.runningAction;
        if (signaller != null) {
            signaller.cancel();
        }
    }

    public Executor getCriticalSectionExecutor() {
        assertOpen();
        return this.criticalSectionExecutor;
    }

    public void markDirty(StatusAdvancingScheduler<K, V, Ctx, UserData> scheduler) {
        Assertions.assertTrue(tryMarkDirty(scheduler));
    }

    public boolean tryMarkDirty(StatusAdvancingScheduler<K, V, Ctx, UserData> scheduler) {
        long state = (long) VH_STATE.getOpaque(this); // InitAuther97: must do actual loading
        if ((state & FLAG_REMOVED) != 0) {
            return false;
        }
        if ((state & FLAG_DIRTY) != 0) {
            return true;
        }
        state = (long) VH_STATE.getAndBitwiseOrAcquire(this, FLAG_DIRTY);
        if ((state & FLAG_REMOVED) != 0) {
            return false;
        }
        if ((state & FLAG_DIRTY) != 0) {
            return true;
        }
        scheduleTick(scheduler);
        return true;
    }

    private void scheduleTick(StatusAdvancingScheduler<K, V, Ctx, UserData> scheduler) {
        this.criticalSectionExecutor.execute(() -> {
            clearFlag(FLAG_DIRTY);
            scheduler.tickHolder0(this);
        });
    }

    /// Whether downgrading can proceed
    private boolean casStateDowngrade(byte toStatus) {
        long state = loState();
        if (getTargetStatus(state) > toStatus) {
            return false;
        }
        while (!casRelStatus(state, toStatus)) {
            Thread.onSpinWait();
            state = loState();
            if (getTargetStatus(state) > toStatus) {
                return false;
            }
        }
        return true;
    }

    private void casStateAdvance(byte newStatus) {
        long state = lpState();
        while (!casRelStatus(state, newStatus)) {
            Thread.onSpinWait();
            state = lpState();
        }
    }

    public boolean setStatusDowngrade(ItemStatus<K, V, Ctx> status) {
        assertOpen();
        final byte ordinal = status.getOrdinal();
        final long state = lpState();
        final var current = unloadedStatus.getAt(getStatus(state));
        Assertions.assertTrue(status.getNext() == current, "Invalid status downgrade");
        if (!casStateDowngrade(ordinal)) {
            return false;
        }
        // Ensure that we see a properly initialized future
        // final var future = (CompletableFuture<?>) VH_FUTURES.getAndSetAcquire(this.futures, ordinal, UNLOADED_FUTURE);
        // future.completeExceptionally(UNLOADED_EXCEPTION);
        return true;
    }

    public void setStatusAdvance(ItemStatus<K, V, Ctx> status) {
        assertOpen();
        final byte ordinal = status.getOrdinal();
        final var current = getStatus0();
        Assertions.assertTrue(status.getPrev() == current, "Invalid status upgrade");
        final ItemTicket[] ticketsToFire;
        synchronized (this) {
            casStateAdvance(ordinal);
            ticketsToFire = this.tickets[ordinal].toArray(ItemTicket[]::new);
        }
        final var futureToFire = this.futures[ordinal];
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < ticketsToFire.length; i++) {
            ticketsToFire[i].consumeCallback();
        }
        futureToFire.complete(null);
    }

    public void setStatusForDowngradeCancellation(ItemStatus<K, V, Ctx> status) {
        assertOpen();
        final byte ordinal = status.getOrdinal();
        final var current = getStatus0();
        Assertions.assertTrue(status.getPrev() == current, "Invalid status upgrade");
        casStateAdvance(ordinal);
        final var futureToFire = this.futures[ordinal];
        Assertions.assertTrue(futureToFire != UNLOADED_FUTURE);
        Assertions.assertTrue(!futureToFire.isDone());
        futureToFire.complete(null);
    }

    public ItemStatus<K, V, Ctx> getStatus() {
        return unloadedStatus.getAt(getStatus(loState()));
    }

    public ItemStatus<K, V, Ctx> getStatus0() {
        return unloadedStatus.getAt(getStatus(lpState()));
    }

    public synchronized void setDependencies(ItemStatus<K, V, Ctx> status, KeyStatusPair<K, V, Ctx>[] dependencies) {
        assertOpen();
        final int ordinal = status.getOrdinal();
        if (dependencies != null) {
            Assertions.assertTrue(this.requestedDependencies[ordinal] == null, "Duplicate setDependencies call");
            this.requestedDependencies[ordinal] = dependencies;
        } else {
            Assertions.assertTrue(this.requestedDependencies[ordinal] != null, "Duplicate setDependencies call");
            this.requestedDependencies[ordinal] = null;
        }
    }

    public synchronized KeyStatusPair<K, V, Ctx>[] getDependencies(ItemStatus<K, V, Ctx> status) {
        assertOpen();
        return this.requestedDependencies[status.getOrdinal()];
    }

    public K getKey() {
        return this.key;
    }

    public CompletableFuture<?> getFutureForStatus(ItemStatus<K, V, Ctx> status) {
        return getFutureForStatus0(status).copy();
    }

    /**
     * Only for trusted methods
     */
    public CompletableFuture<?> getFutureForStatus0(ItemStatus<K, V, Ctx> status) {
        final byte ordinal = status.getOrdinal();
        return this.futures[ordinal];
    }

    public void setItem(V item) {
        this.item = item;
    }

    /// Access mode: plain, piggyback on status when accessed off scheduling
    public V getItem() {
        return this.item;
    }

    /**
     * Get the user data of this item.
     * Access mode: plain, because it's a stable value.
     *
     * @apiNote it is the caller's obligation to ensure the holder is not closed
     * @return the user data
     */
    public UserData getUserData() {
        return this.userData;
    }

    /// Note: access mode is plain, make sure there is proper synchronization before sharing.
    public void setUserData(UserData userData) {
        this.userData = userData;
    }

    /// Access mode: plain
    public long getFlagsPlain() {
        return (long) VH_STATE.get(this) & -1 >>> Long.SIZE - ItemStatus.STATUS_LENGTH - ItemStatus.STATUS_SIZE * 2;
    }

    /// Access mode: plain
    public long setFlag(long flag) {
        assertOpen();
        // Make sure to use release if you want to broadcast changes via flags
        return (long) VH_STATE.getAndBitwiseOrAcquire(this, flag);
    }

    /**
     * Note: do not use this unless you know what you are doing
     * Access mode: plain
     */
    public void clearFlag(long flag) {
        Assertions.assertTrue((flag & FLAG_REMOVED) == 0, "Cannot clear FLAG_REMOVED");
        assertOpen();
        andStateRelaxed(~flag);
    }

    boolean release(long state) {
        // Don't change it to getAndBitwiseOr, as logic in add/remove ticket never checked for holder's availability
        // We are not in a hurry to remove a holder! Ticket operations are complex enough!
        // If you bear to do this, you are just pissing in the wind!
        return VH_STATE.weakCompareAndSetAcquire(this, state, state | FLAG_FREE | FLAG_REMOVED);
    }

    public void addDependencyTicket(StatusAdvancingScheduler<K, V, Ctx, ?> scheduler, K key, ItemStatus<K, V, Ctx> status, ItemTicket ticket) {
        synchronized (this.dependencyRefCnts) {
            final int[] refCnt = this.dependencyRefCnts.computeIfAbsent(key, this.depRefCntCreate);
            final int ordinal = status.getOrdinal();
            if (refCnt[ordinal] == -1) {
                refCnt[ordinal] = 0;
                scheduler.addTicket0(key, status, ticket);
            } else {
                ticket.consumeCallback();
            }
            refCnt[ordinal] ++;
        }
    }

    public void removeDependencyTicket(K key, ItemStatus<K, V, Ctx> status) {
        synchronized (this.dependencyRefCnts) {
            final int[] refCnt = this.dependencyRefCnts.get(key);
            Assertions.assertTrue(refCnt != null);
            assert refCnt != null;
            final int old = refCnt[status.getOrdinal()]--;
            Assertions.assertTrue(old > 0);
            if (old == 1) {
                dependencyDirty = true;
            }
        }
    }

    public boolean isDependencyDirty() {
        synchronized (this.dependencyRefCnts) {
            return this.dependencyDirty;
        }
    }

    public boolean holdsDependency() {
        synchronized (this.dependencyRefCnts) {
            for (ObjectBidirectionalIterator<Object2ReferenceMap.Entry<K, int[]>> iterator = this.dependencyRefCnts.object2ReferenceEntrySet().fastIterator(); iterator.hasNext(); ) {
                final Object2ReferenceMap.Entry<K, int[]> entry = iterator.next();
                int[] refCnt = entry.getValue();
                for (int i : refCnt) {
                    if (i != -1) return true;
                }
            }
            return false;
        }
    }

    public void flushDependencyCache0(StatusAdvancingScheduler<K, V, Ctx, ?> scheduler) {
        synchronized (this.dependencyRefCnts) {
            if (!dependencyDirty) return;
            for (ObjectBidirectionalIterator<Object2ReferenceMap.Entry<K, int[]>> iterator = this.dependencyRefCnts.object2ReferenceEntrySet().fastIterator(); iterator.hasNext(); ) {
                Object2ReferenceMap.Entry<K, int[]> entry = iterator.next();
                final K key = entry.getKey();
                int[] refCnt = entry.getValue();
                boolean isEmpty = true;
                for (byte ordinal = 0, refCntLength = (byte) refCnt.length; ordinal < refCntLength; ordinal++) {
                    if (refCnt[ordinal] == 0) {
                        scheduler.removeTicket(key, ItemTicket.TicketType.DEPENDENCY, this.getKey(), this.unloadedStatus.getAt(ordinal));
                        refCnt[ordinal] = -1;
                    }
                    if (refCnt[ordinal] != -1) isEmpty = false;
                }
                if (isEmpty)
                    iterator.remove();
            }
            dependencyDirty = false;
        }
    }

    private void assertOpen() {
        Assertions.assertTrue(isOpen());
    }

    public boolean isOpen() {
        return (this.getFlagsPlain() & FLAG_REMOVED) == 0;
    }

}
