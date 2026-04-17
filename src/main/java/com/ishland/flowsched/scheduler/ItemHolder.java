package com.ishland.flowsched.scheduler;

import com.ishland.flowsched.structs.OneTaskAtATimeExecutor;
import com.ishland.flowsched.util.Assertions;
import io.reactivex.rxjava3.core.Completable;
import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.objects.*;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
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
    /// flag_busy (1bit) | flag_dirty (1bit) | flag_broken (1bit) | flag_removed (1bit) | changing status (5bit) | status (5bit) | ticket bitset (32bit)
    protected volatile long state = 1; // Core synchronization point, responsible for upgrade/downgrade/future
    private long l11, l12, l13, l14, l15, l16, l17; // padding
}

public class ItemHolder<K, V, Ctx, UserData> extends ItemHolderHotField {

    // private static final VarHandle VH_SCHEDULED_DIRTY;
    static final VarHandle VH_STATE;

    public static final long FLAG_REMOVED = 1L << 42;
    /**
     * Indicates the holder have been marked broken
     * If set, the holder:
     * - will not be allowed to be upgraded any further
     * - will still be allowed to be downgraded, but operations to it should be careful
     */
    public static final long FLAG_BROKEN = 1L << 43;

    public static final long FLAG_DIRTY = 1L << 44;

    public static final long FLAG_BUSY = 1L << 45;

    static {
        try {
            final var lookup = MethodHandles.lookup();
            // VH_SCHEDULED_DIRTY = lookup.findVarHandle(ItemHolder.class, "scheduledDirty", int.class);
            VH_STATE = lookup.findVarHandle(ItemHolder.class, "state", long.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private final K key;
    private final ItemStatus<K, V, Ctx> unloadedStatus;
    private final byte unloadedOrdinal;
    private final Set<ItemTicket>[] tickets;
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
    private final OneTaskAtATimeExecutor criticalSectionExecutor;

    private final CompletableFuture<?>[] futures; // Futures to fire by setStatus, only written by ticket ops threads
    private V item; // Piggyback on state read when read off scheduler threads
    private UserData userData; // Stable value
    private Pair<Cancellable, ItemStatus<K, V, Ctx>> runningAction = null; // Only used by scheduler threads
    private boolean dependencyDirty = false; // Used in dependency critical section

    ItemHolder(ItemStatus<K, V, Ctx> initialStatus, K key, ObjectFactory objectFactory, Executor backgroundExecutor) {
        this.unloadedStatus = Objects.requireNonNull(initialStatus);
        this.unloadedOrdinal = initialStatus.getOrdinal();
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
        this.depRefCntCreate =k -> {
            int[] refCnt = new int[length];
            Arrays.fill(refCnt, -1);
            return refCnt;
        };
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

    static long undirty(long state) {
        return state & ~FLAG_DIRTY;
    }

    boolean casRelStatus(long expected, byte status) {
        return VH_STATE.weakCompareAndSetRelease(this, expected, withStatus(expected, status));
    }

    boolean casStatePlain(long expected, long next) {
        return VH_STATE.weakCompareAndSetPlain(this, expected, next);
    }

    long andStatePlain(long and) {
        return (long) VH_STATE.getAndBitwiseAndAcquire(this, and);
    }

    long setTargetRelease(byte ordinal) {
        return (long) VH_STATE.getAndBitwiseOrRelease(this, 1L << ordinal);
    }

    long unsetTargetRelease(byte ordinal) {
        return (long) VH_STATE.getAndBitwiseAndRelease(this, ~(1L << ordinal));
    }

    long loState() {
        return (long) VH_STATE.getAcquire(this);
    }

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
        if ((state & FLAG_BUSY) != 0) return false;
        return casStatePlain(state, state | FLAG_BUSY);
    }

    void unlockScheduler() {
        andStatePlain(~FLAG_BUSY);
    }

    public ItemStatus<K, V, Ctx> changingStatusTo() {
        assertOpen();
        final Pair<Cancellable, ItemStatus<K, V, Ctx>> pair = this.runningAction;
        return pair != null ? pair.right() : null;
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

            // InitAuther97: target status is only changed by tickets thread
            // which is properly synchronized via synchronized block
            // Memory ordering: release, check when removing synchronization
            futuresToFail = new CompletableFuture[oldTarget - newTarget];
            for (int i = newTarget + 1; i <= oldTarget; i++) {
                // InitAuther97: use swap because of possible racing set.
                // Acquire ensures that we see a properly initialized future.
                // Writes to futures are all guarded with synchronization on ticket sets,
                // so they are always witnessed in the program order.
                futuresToFail[i - newTarget - 1] = this.futures[i];
                this.futures[i] = UNLOADED_FUTURE;
            }
            unsetTargetRelease(ordinal);
        }

        // InitAuther97: now we fail any future that either exists before removing
        // or gets stuffed into the array when removing
        // noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < futuresToFail.length; i++) {
            futuresToFail[i].completeExceptionally(UNLOADED_EXCEPTION);
        }
    }

    public void subscribeOp(Completable op, StatusAdvancingScheduler<K, V, Ctx, UserData> scheduler) {
        assertOpen();
        setFlag(FLAG_DIRTY);
        op.subscribe(() -> {
            unlockScheduler();
            scheduleTick(scheduler);
        }, t -> {
            unlockScheduler();
            if (t instanceof SkipSchedulingException) return;
            scheduleTick(scheduler);
        });
    }

    // sync externally
    public void finishAction() {
        assertOpen();
        Assertions.assertTrue(this.runningAction != null, "No action is present when trying to finish an action");
        this.runningAction = null;
    }

    // sync externally
    public void submitAction(Cancellable cancellation, ItemStatus<K, V, Ctx> status) {
        assertOpen();
        Assertions.assertTrue(this.runningAction == null, "Only one action can happen at a time");
        this.runningAction = Pair.of(cancellation, status);
    }

    // sync externally
    public void tryCancelAction() {
        assertOpen();
        final Pair<Cancellable, ItemStatus<K, V, Ctx>> signaller = this.runningAction;
        if (signaller != null) {
            signaller.left().cancel();
        }
    }

    public Executor getCriticalSectionExecutor() {
        assertOpen();
        return this.criticalSectionExecutor;
    }

    public void executeCriticalSectionAndBusy(Runnable command) {
        assertOpen();
        this.getCriticalSectionExecutor().execute(command);
    }

    public void markDirty(StatusAdvancingScheduler<K, V, Ctx, UserData> scheduler) {
        Assertions.assertTrue(tryMarkDirty(scheduler));
    }

    public boolean tryMarkDirty(StatusAdvancingScheduler<K, V, Ctx, UserData> scheduler) {
        long state = (long) VH_STATE.get(this);
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

    public void setStatusAdvance(ItemStatus<K, V, Ctx> status, boolean isCancellation) {
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
        if (isCancellation) {
            Assertions.assertTrue(futureToFire != UNLOADED_FUTURE);
            Assertions.assertTrue(futureToFire == null || !futureToFire.isDone());
        }
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < ticketsToFire.length; i++) {
            ticketsToFire[i].consumeCallback();
        }
        if (futureToFire != null) futureToFire.complete(null);
    }

    public ItemStatus<K, V, Ctx> getStatus() {
        return unloadedStatus.getAt(getStatus(loState()));
    }

    public ItemStatus<K, V, Ctx> getStatus0() {
        return unloadedStatus.getAt(getStatus(lpState()));
    }

    void flushUnloadedStatus(ItemStatus<K, V, Ctx> currentStatus) {
        ArrayList<CompletableFuture<?>> futuresToFire = null;
        if (currentStatus.getNext() == null) {
            return;
        }
        synchronized (this.tickets) {
            ItemStatus<K, V, Ctx> targetStatus = this.getTargetStatus();
            if (targetStatus.getNext() == null) {
                return;
            }
            for (int i = Math.max(currentStatus.getOrdinal(), targetStatus.getOrdinal()) + 1; i < this.futures.length; i ++) {
                if (futuresToFire == null) futuresToFire = new ArrayList<>();
                CompletableFuture<?> oldFuture = this.futures[i];
                futuresToFire.add(oldFuture);
                this.futures[i] = UNLOADED_FUTURE;
            }
        }
        if (futuresToFire != null) {
            for (int i = 0, finalFuturesToFireSize = futuresToFire.size(); i < finalFuturesToFireSize; i++) {
                CompletableFuture<?> future = futuresToFire.get(i);
                future.completeExceptionally(UNLOADED_EXCEPTION);
            }
        }
    }

    void validateCompletedFutures(ItemStatus<K, V, Ctx> current) {
        synchronized (this.tickets) {
            for (int i = this.unloadedOrdinal + 1; i <= current.getOrdinal(); i++) {
                CompletableFuture<?> future = this.futures[i];
                Assertions.assertTrue(future != UNLOADED_FUTURE, "Future for loaded status cannot be UNLOADED_FUTURE");
                Assertions.assertTrue(future.isDone(), "Future for loaded status must be completed");
            }
        }
    }

    void validateAllFutures() {
        synchronized (this.tickets) {
            for (int i = this.unloadedOrdinal + 1; i < this.futures.length; i++) {
                CompletableFuture<?> future = this.futures[i];
                if (i <= this.getStatus0().getOrdinal()) {
                    Assertions.assertTrue(future.isDone(), "Future for loaded status must be completed");
                }
                if (i <= this.getTargetStatus().getOrdinal()) {
                    Assertions.assertTrue(future != UNLOADED_FUTURE, "Future for requested status cannot be UNLOADED_FUTURE");
                } else {
                    Assertions.assertTrue(future == UNLOADED_FUTURE, "Future for non-requested status must be UNLOADED_FUTURE");
                }
            }
        }
    }

    void validateRequestedFutures(ItemStatus<K, V, Ctx> current) {
        synchronized (this.tickets) {
            for (int i = this.unloadedOrdinal + 1; i <= current.getOrdinal(); i++) {
                CompletableFuture<?> future = this.futures[i];
                Assertions.assertTrue(future != UNLOADED_FUTURE, "Future for requested status cannot be UNLOADED_FUTURE");
            }
        }
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
    public void setFlag(long flag) {
        assertOpen();
        // Make sure to use release if you want to broadcast changes via flags
        VH_STATE.getAndBitwiseOrAcquire(this, flag);
    }

    /**
     * Note: do not use this unless you know what you are doing
     * Access mode: plain
     */
    public void clearFlag(long flag) {
        Assertions.assertTrue((flag & FLAG_REMOVED) == 0, "Cannot clear FLAG_REMOVED");
        assertOpen();
        VH_STATE.getAndBitwiseAndAcquire(this, ~flag);
    }

    boolean release(long state) {
        return VH_STATE.weakCompareAndSetAcquire(this, state, (state & ~FLAG_BUSY) | FLAG_REMOVED);
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

    public void scheduleFlushDependencyCache(StatusAdvancingScheduler<K, V, Ctx, ?> scheduler) {
        this.executeCriticalSectionAndBusy(() -> this.flushDependencyCache0(scheduler));
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
