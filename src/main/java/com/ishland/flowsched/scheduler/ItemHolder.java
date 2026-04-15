package com.ishland.flowsched.scheduler;

import com.ishland.flowsched.structs.OneTaskAtATimeExecutor;
import com.ishland.flowsched.util.Assertions;
import io.reactivex.rxjava3.core.Completable;
import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.objects.Object2ReferenceFunction;
import it.unimi.dsi.fastutil.objects.Object2ReferenceLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ReferenceMap;
import it.unimi.dsi.fastutil.objects.ObjectBidirectionalIterator;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

@SuppressWarnings("unused")
class ItemHolderHotField {
    /// empty | flag_dirty (1bit) | flag_broken (1bit) | flag_removed (1bit) | target status (8bit) | changing status (8bit) | status (8bit)
    protected volatile int state;

    protected volatile int scheduledDirty = 0; // Used by external threads
}

public class ItemHolder<K, V, Ctx, UserData> extends ItemHolderHotField {

    // private static final VarHandle VH_SCHEDULED_DIRTY;
    static final VarHandle VH_STATE;
    private static final VarHandle VH_FUTURES = MethodHandles.arrayElementVarHandle(CompletableFuture[].class);

    public static final IllegalStateException UNLOADED_EXCEPTION = new IllegalStateException("Not loaded");
    private static final CompletableFuture<?> UNLOADED_FUTURE = CompletableFuture.failedFuture(UNLOADED_EXCEPTION);
    private static final CompletableFuture<?> COMPLETED_VOID_FUTURE = CompletableFuture.completedFuture(null);

    public static final int FLAG_REMOVED = 1 << 24;
    /**
     * Indicates the holder have been marked broken
     * If set, the holder:
     * - will not be allowed to be upgraded any further
     * - will still be allowed to be downgraded, but operations to it should be careful
     */
    public static final int FLAG_BROKEN = 1 << 25;

    public static final int FLAG_DIRTY = 1 << 26;

    static {
        try {
            final var lookup = MethodHandles.lookup();
            // VH_SCHEDULED_DIRTY = lookup.findVarHandle(ItemHolder.class, "scheduledDirty", int.class);
            VH_STATE = lookup.findVarHandle(ItemHolder.class, "state", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private final K key;
    private final ItemStatus<K, V, Ctx> unloadedStatus;
    private final byte unloadedOrdinal;
    private final BusyRefCounter busyRefCounter = new BusyRefCounter();
    private final TicketSet<K, V, Ctx> tickets;
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

    private volatile int state; // Core synchronization point, responsible for upgrade/downgrade/future
    private final CompletableFuture<?>[] futures; // Futures to fire by setStatus, only written by ticket ops threads
    private V item; // Piggyback on state read when read off scheduler threads
    private UserData userData; // Stable value
    private Pair<Cancellable, ItemStatus<K, V, Ctx>> runningAction = null; // Only used by scheduler threads
    private boolean dependencyDirty = false; // Used in dependency critical section

    ItemHolder(ItemStatus<K, V, Ctx> initialStatus, K key, ObjectFactory objectFactory, Executor backgroundExecutor) {
        this.unloadedStatus = Objects.requireNonNull(initialStatus);
        this.unloadedOrdinal = initialStatus.getOrdinal();
        this.key = Objects.requireNonNull(key);
        this.tickets = new TicketSet<>(this.unloadedStatus, objectFactory);

        ItemStatus<K, V, Ctx>[] allStatuses = initialStatus.getAllStatuses();
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

    static byte getTargetStatus(int state) {
        return (byte) (state >>> (ItemStatus.STATUS_SIZE << 1) & ItemStatus.STATUS_MASK);
    }

    static int withTargetStatus(int state, byte targetStatus) {
        return state & ~(ItemStatus.STATUS_MASK << (ItemStatus.STATUS_SIZE << 1)) | targetStatus << (ItemStatus.STATUS_SIZE << 1);
    }

    static byte getStatus(int state) {
        return (byte) (state & ItemStatus.STATUS_MASK);
    }

    static int withStatus(int state, byte status) {
        return state & ~ItemStatus.STATUS_MASK | status;
    }

    static byte getNextStatus(int state) {
        return (byte) (state >>> ItemStatus.STATUS_SIZE & ItemStatus.STATUS_MASK);
    }

    static int withNextStatus(int state, byte nextStatus) {
        return state & ~(ItemStatus.STATUS_MASK << ItemStatus.STATUS_SIZE) | nextStatus << (ItemStatus.STATUS_SIZE << 1);
    }

    boolean casRelTarget(int expected, byte targetStatus) {
        return VH_STATE.weakCompareAndSetRelease(this, expected, withTargetStatus(expected, targetStatus));
    }

    boolean casRelStatus(int expected, byte status) {
        return VH_STATE.weakCompareAndSetRelease(this, expected, withStatus(expected, status));
    }

    int loState() {
        return (int) VH_STATE.getAcquire(this);
    }

    int lpState() {
        return (int) VH_STATE.get(this);
    }

    /**
     * Not thread-safe, protect with statusMutex
     */
    private void createFutures(byte from, byte to) {
        for (int i = from + 1; i <= to; i++) {
            if (this.futures[i] != UNLOADED_FUTURE) {
                failCreateFutures(this.futures[i]);
            }
            this.futures[i] = null;
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

    public synchronized boolean isBusy() {
        assertOpen();
        return busyRefCounter.isBusy();
    }

    public ItemStatus<K, V, Ctx> changingStatusTo() {
        assertOpen();
        final Pair<Cancellable, ItemStatus<K, V, Ctx>> pair = this.runningAction;
        return pair != null ? pair.right() : null;
    }

    private boolean updateTargetStatus(int state, byte target) {
        byte initial = target;
        // Spin update target status
        // Key information is current >= target (for early cancellation), thus need release for target update
        while (!casRelTarget(state, target)) {
            Thread.onSpinWait();
            state = lpState();
            // check for removed flag to break away
            // may happen if lose race to scheduling
            if ((state & FLAG_REMOVED) != 0) {
                return false;
            }
            // get latest target status and retry
            // load ordered is used to propagate hb relationship
            target = this.tickets.loTargetStatus();
            // someone has done it, exit
            if (getTargetStatus(state) == target) break;
            if (target < initial)
                System.err.println("Got smaller target status than initial");
        }
        return true;
    }

    private boolean rescueHolder(ItemStatus<K, V, Ctx> targetStatus, int state) {
        final byte target = targetStatus.getOrdinal();
        while (!casRelTarget(state, target)) {
            Thread.onSpinWait();
            state = lpState();
            if ((state & FLAG_REMOVED) != 0) return false;
            if (getTargetStatus(state) != unloadedOrdinal) break;
        }
        return true;
    }

    public boolean addTicket(ItemStatus<K, V, Ctx> targetStatus, ItemTicket ticket) {
        Objects.requireNonNull(ticket);
        int state = lpState();
        if ((state & FLAG_REMOVED) != 0 ||
                getTargetStatus(state) == unloadedOrdinal && !rescueHolder(targetStatus, state)) {
            return false;
        }
        final byte newTarget;
        boolean success;
        synchronized (this.tickets) {
            state = lpState();
            if ((state & FLAG_REMOVED) != 0) {
                return false;
            }
            final byte oldTarget = this.tickets.lpTargetStatus();
            final boolean add = this.tickets.checkAdd(targetStatus, ticket);
            if (!add) {
                throw new IllegalStateException("Ticket already exists");
            }
            newTarget = this.tickets.addUnchecked(targetStatus);
            if (oldTarget != newTarget) {
                createFutures(oldTarget, newTarget);
            }
            success = updateTargetStatus(state, newTarget);
        }
        if (success) {
            byte target = targetStatus.getOrdinal();
            final byte current = getStatus(state);
            final byte projected = getNextStatus(state);
            if (current > target || (current == target && projected >= target)) {
                ticket.consumeCallback();
            }
        }
        return success;
    }

    public void removeTicket(ItemStatus<K, V, Ctx> targetStatus, ItemTicket ticket) {
        assertOpen();
        CompletableFuture<?>[] futuresToFail;
        final byte newTarget;
        synchronized (this.tickets) {
            final byte oldTarget = this.tickets.lpTargetStatus();
            final boolean remove = this.tickets.checkRemove(targetStatus, ticket);
            if (!remove) {
                throw new IllegalStateException("Ticket does not exist");
            }
            newTarget = this.tickets.removeUnchecked(targetStatus);
            // InitAuther97: target status is only changed by tickets thread
            // which is properly synchronized via synchronized block
            // Memory ordering: plain, check when removing synchronization
            if (oldTarget == newTarget) {
                return;
            }
            futuresToFail = new CompletableFuture[oldTarget - newTarget];
            for (int i = newTarget + 1; i <= oldTarget; i++) {
                // InitAuther97: use swap because of possible racing set.
                // Acquire ensures that we see a properly initialized future.
                // Writes to futures are all guarded with synchronization on ticket sets,
                // so they are always witnessed in the program order.
                final var swap = VH_FUTURES.getAndSetAcquire(this.futures, i, UNLOADED_FUTURE);
                futuresToFail[i - newTarget - 1] = (CompletableFuture<?>) swap;
            }
            // InitAuther97: the affected futures are all masked by getFutureForStatus0 to UNLOADED_FUTURE.
            // if they unfortunately modify the futures (inserting a new one), it will be completed exceptionally later.
            // Release semantics is used here to support the use of state check as a synchronization point
            updateTargetStatus(lpState(), newTarget);
        }
        // InitAuther97: now we fail any future that either exists before removing
        // or gets stuffed into the array when removing
        // noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < futuresToFail.length; i++) {
            final var future = futuresToFail[i];
            if (future != null) future.completeExceptionally(UNLOADED_EXCEPTION);
        }
    }

    public void submitOp(CompletionStage<Void> op) {
        assertOpen();
//        this.opFuture.set(opFuture.get().thenCombine(op, (a, b) -> null).handle((o, throwable) -> null));
//        this.opFuture.getAndUpdate(future -> future.thenCombine(op, (a, b) -> null).handle((o, throwable) -> null));
        this.busyRefCounter.incrementRefCount();
        op.whenComplete((_, _) -> this.busyRefCounter.decrementRefCount());
    }

    public void subscribeOp(Completable op) {
        assertOpen();
        this.busyRefCounter.incrementRefCount();
        op.onErrorComplete().subscribe(this.busyRefCounter::decrementRefCount);
    }

    BusyRefCounter busyRefCounter() {
        return this.busyRefCounter;
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

    public CompletableFuture<?> getOpFuture() { // best-effort
        assertOpen();
        if (!this.busyRefCounter.isBusy()) {
            return COMPLETED_VOID_FUTURE;
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        this.busyRefCounter.addListener(() -> future.complete(null));
        return future;
    }

    public void submitOpListener(Runnable runnable) {
        assertOpen();
        this.busyRefCounter.addListener(runnable);
    }

    public void consolidateMarkDirty(StatusAdvancingScheduler<K, V, Ctx, UserData> scheduler) {
        assertOpen();
        this.busyRefCounter.addListenerOnce(() -> this.markDirty(scheduler));
    }

    public Executor getCriticalSectionExecutor() {
        assertOpen();
        return this.criticalSectionExecutor;
    }

    public void executeCriticalSectionAndBusy(Runnable command) {
        assertOpen();
        this.busyRefCounter().incrementRefCount();
        this.getCriticalSectionExecutor().execute(() -> {
            try {
                command.run();
            } finally {
                this.busyRefCounter().decrementRefCount();
            }
        });
    }

    public void markDirty(StatusAdvancingScheduler<K, V, Ctx, UserData> scheduler) {
        Assertions.assertTrue(tryMarkDirty(scheduler));
    }

    public boolean tryMarkDirty(StatusAdvancingScheduler<K, V, Ctx, UserData> scheduler) {
        final int state = (int) VH_STATE.getAndBitwiseOrAcquire(this, FLAG_DIRTY);
        if ((state & FLAG_REMOVED) != 0) {
            return false;
        }
        if ((state & FLAG_DIRTY) != 0) {
            return true;
        }
        this.criticalSectionExecutor.execute(() -> {
            VH_STATE.getAndBitwiseAndRelease(this, ~FLAG_DIRTY);
            scheduler.tickHolder0(this);
        });
        return true;
    }

    /// Whether downgrading can proceed
    private boolean casStateDowngrade(byte toStatus) {
        int state = loState();
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
        int state = lpState();
        while (!casRelStatus(state, newStatus)) {
            Thread.onSpinWait();
            state = lpState();
        }
    }

    public boolean setStatusDowngrade(ItemStatus<K, V, Ctx> status) {
        assertOpen();
        final byte ordinal = status.getOrdinal();
        final int state = lpState();
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
        final var current = getStatus0();
        Assertions.assertTrue(status.getPrev() == current, "Invalid status upgrade");
        final ItemTicket[] ticketsToFire;
        final CompletableFuture<?> futureToFire;
        casStateAdvance(status.getOrdinal());
        futureToFire = (CompletableFuture<?>) VH_FUTURES.getAcquire(this.futures, status.getOrdinal());
        synchronized (this.tickets) {
            ticketsToFire = this.tickets.getTicketsForStatus(status).toArray(ItemTicket[]::new);
        }
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
        int state = (int) VH_STATE.getAcquire(this);
        final byte target = getTargetStatus(state);
        final byte current = getStatus(state);
        if (target < ordinal) {
            return UNLOADED_FUTURE;
        } else if (ordinal < current) {
            return COMPLETED_VOID_FUTURE;
        }
        final var future = (CompletableFuture<?>) VH_FUTURES.get(this.futures, ordinal);
        if (future != null) {
            return future;
        }
        final var newFuture = new CompletableFuture<>();
        final var witness = (CompletableFuture<?>) VH_FUTURES.compareAndExchangeRelease(this.futures, ordinal, null, newFuture);
        if (witness != null) {
            return witness;
        }
        state = (int) VH_STATE.getAcquire(this);
        if (getStatus(state) >= ordinal) {
            newFuture.complete(null);
        }
        return newFuture;
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
    public int getFlagsPlain() {
        return (int) VH_STATE.get(this) & ~(0xFF << ItemStatus.STATUS_SIZE * 3);
    }

    /// Access mode: plain
    public void setFlag(int flag) {
        assertOpen();
        // Make sure to use release if you want to broadcast changes via flags
        VH_STATE.getAndBitwiseOrAcquire(this, flag);
    }

    /**
     * Note: do not use this unless you know what you are doing
     * Access mode: plain
     */
    public void clearFlag(int flag) {
        Assertions.assertTrue((flag & FLAG_REMOVED) == 0, "Cannot clear FLAG_REMOVED");
        assertOpen();
        VH_STATE.getAndBitwiseAndAcquire(this, ~flag);
    }

    boolean release(int state) {
        return VH_STATE.weakCompareAndSetAcquire(this, state, state | FLAG_REMOVED);
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
