package com.ishland.flowsched.scheduler;

import com.ishland.flowsched.util.Assertions;
import com.ishland.flowsched.util.Constant;
import io.reactivex.rxjava3.core.Completable;
import it.unimi.dsi.fastutil.objects.Object2ReferenceOpenHashMap;

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.StampedLock;

/**
 * A scheduler that advances status of items.
 *
 * @param <K> the key type
 * @param <V> the item type
 * @param <Ctx> the context type
 */
public abstract class StatusAdvancingScheduler<K, V, Ctx, UserData> {

    /// InitAuther97: ItemHolder object initialization is guarded by itemsLock,
    /// make sure to use proper synchronization before changing critical sections
    private final StampedLock itemsLock = new StampedLock();
    private final Object2ReferenceOpenHashMap<K, ItemHolder<K, V, Ctx, UserData>> items = new Object2ReferenceOpenHashMap<>() {
        @Override
        protected void rehash(int newN) {
            if (n < newN) {
                super.rehash(newN);
            }
        }
    };
    private final ObjectFactory objectFactory;

    private static final int FLAG_FLUSH_DEPENDENCY = 1;

    protected StatusAdvancingScheduler() {
        this(new ObjectFactory.DefaultObjectFactory());
    }

    protected StatusAdvancingScheduler(ObjectFactory objectFactory) {
        this.objectFactory = Objects.requireNonNull(objectFactory);
    }

    protected abstract Executor getBackgroundExecutor();

    protected abstract ItemStatus<K, V, Ctx> getUnloadedStatus();

    protected abstract Ctx makeContext(ItemHolder<K, V, Ctx, UserData> holder, ItemStatus<K, V, Ctx> nextStatus, KeyStatusPair<K, V, Ctx>[] dependencies, boolean isUpgrade);

    protected ExceptionHandlingAction handleTransactionException(ItemHolder<K, V, Ctx, UserData> holder, ItemStatus<K, V, Ctx> nextStatus, boolean isUpgrade, Throwable throwable) {
        //noinspection CallToPrintStackTrace
        throwable.printStackTrace();
        return ExceptionHandlingAction.MARK_BROKEN;
    }

    protected void handleUnrecoverableException(Throwable throwable) {
        //noinspection CallToPrintStackTrace
        throwable.printStackTrace();
    }

    /**
     * Called when an item is constructed.
     *
     * @implNote This method is called before the item is added to the internal map. Make sure to not access the item from the map.
     *           May get called from any thread. May get called multiple times for the same item in a race.
     *           The item constructed may not be the final item in the map.
     * @param holder the item being constructed
     */
    protected void onItemConstruct(ItemHolder<K, V, Ctx, UserData> holder) {
    }

    /**
     * Called when an item is created.
     *
     * @implNote This method is called after the item is added to the internal map.
     *           May get called from any thread. This should return ASAP because it holds global write lock.
     * @param holder the item being added
     */
    protected void onItemCreation(ItemHolder<K, V, Ctx, UserData> holder) {
    }

    /**
     * Called when an item is deleted.
     *
     * @implNote This method is called when the monitor of the holder is held.
     * @param holder the item being removed
     */
    protected void onItemRemoval(ItemHolder<K, V, Ctx, UserData> holder) {
    }

    protected void onItemUpgrade(ItemHolder<K, V, Ctx, UserData> holder, ItemStatus<K, V, Ctx> statusReached) {
    }

    protected void onItemDowngrade(ItemHolder<K, V, Ctx, UserData> holder, ItemStatus<K, V, Ctx> statusReached) {
    }

    /**
     * Tick goes like this: First, check if the holder is open; if not just return.
     *
     * Then, try to exclusively lock scheduler. This is held during the whole world generation pipeline.
     * If the lock is already held, run tickHolderBusy logic.
     *
     * The scheduling state is determined in an attempt style; only a successful CAS back into the state
     * means that this scheduling is valid, otherwise, updates occurred when scheduling is running, and
     * it's now visible to us; the holder is only marked free again at the end of the pipeline.
     * This consolidates multiple ticket operations, reducing task count on the critical section executor.
     *
     * Operations like flushDependencyCache0, which used to be scheduled directly into the executor, are
     * now implemented as a part of holder tick, which actively checks for such task to run. Since the
     * operations are usually marked to schedulerState within the scheduler thread, update to the field
     * is not atomic, to reduce atomic rmw overhead.
     * @param holder
     */
    void tickHolder0(ItemHolder<K, V, Ctx, UserData> holder) {
        // May happen if removeTicket marks dirty too late while the holder is ticking
        if (!holder.isOpen()) {
            return;
        }
        final K key = holder.getKey();
        long state = holder.lpState();
        if (!holder.tryLockSchedulerRelaxed()) {
            final var next = ItemHolder.getNextStatus(state);
            final var current = ItemHolder.getStatus(state);
            final var target = ItemHolder.getTargetStatus(state);
            if ((next < current && current <= target) || (next > current && current >= target)) holder.tryCancelAction();
            // InitAuther97: If the scheduler is not released, then the update is definitely visible to the scheduler,
            // and will be processed as soon as possible. Removing markDirty as it will waste your power.
            // holder.markDirty(this);
            return;
        }

        doWork(holder);

        final byte currentOrdinal = ItemHolder.getStatus(state);
        final ItemStatus<K, V, Ctx> current = getUnloadedStatus().getAt(currentOrdinal);
        final byte unloadedOrdinal = getUnloadedStatus().getOrdinal();
        final ItemStatus<K, V, Ctx> nextStatus;
        final byte nextOrdinal;
        for (int failures = 0; ; failures++) {
            state = holder.loState();
            final byte targetOrdinal = ItemHolder.getTargetStatus(state);
            final ItemStatus<K, V, Ctx> next = getNextStatus(current, targetOrdinal);
            final byte nextOrdinal0 = next.getOrdinal();
            if (nextOrdinal0 != currentOrdinal) {
                // Change of next status doesn't mean anything. Only the change of current status and next
                // status are synchronization points
                if (!ItemHolder.VH_STATE.weakCompareAndSetPlain(holder, state, ItemHolder.withNextStatus(state, nextOrdinal0))) {
                    // for (int i = 0; i < failures; i++) Thread.onSpinWait();
                    continue;
                }
                nextStatus = next;
                nextOrdinal = nextOrdinal0;
                break;
            }
            if (currentOrdinal != unloadedOrdinal) {
                holder.flushDependencyCache0(this);
                if (!ItemHolder.VH_STATE.weakCompareAndSetPlain(holder, state, state | ItemHolder.FLAG_FREE)) {
                    // for (int i = 0; i < failures; i++) Thread.onSpinWait();
                    continue;
                }
                return;
            }
            if (holder.isDependencyDirty()) {
                holder.flushDependencyCache0(this);
            }
            Assertions.assertTrue(!holder.holdsDependency(), "BUG: %s still holds some dependencies when ready for unloading", holder.getKey());
//          System.out.println("Unloaded: " + key);
            if (!holder.release(state)) {
                // for (int i = 0; i < failures; i++) Thread.onSpinWait();
                continue;
            }
            this.onItemRemoval(holder);
            final long lock = this.itemsLock.writeLock();
            try {
                // Holder may have been removed at this point
                this.items.remove(key, holder);
            } finally {
                this.itemsLock.unlockWrite(lock);
            }
            return;
        }

        Cancellable cancellable = new Cancellable();
        holder.submitAction(cancellable);
        Assertions.assertTrue(holder.getStatus0() == current);
        if (currentOrdinal < nextOrdinal) {
            if ((state & ItemHolder.FLAG_BROKEN) != 0) return;
            advanceStatus0(holder, nextStatus, cancellable);
        } else {
            downgradeStatus0(holder, current, nextStatus, cancellable);
        }
    }

    void doWork(ItemHolder<K, V, Ctx, UserData> holder) {
        final int state = (int) ItemHolder.VH_SCHEDULER_STATE.get(holder);
        if ((state & FLAG_FLUSH_DEPENDENCY) != 0) {
            holder.flushDependencyCache0(this);
        }
        ItemHolder.VH_SCHEDULER_STATE.set(holder, 0);
    }

    private void scheduleFlushDependencyCache(ItemHolder<K, V, Ctx, UserData> holder) {
        ItemHolder.VH_SCHEDULER_STATE.set(holder, FLAG_FLUSH_DEPENDENCY | (int) ItemHolder.VH_SCHEDULER_STATE.get(holder));
    }

    private void downgradeStatus0(ItemHolder<K, V, Ctx, UserData> holder, ItemStatus<K, V, Ctx> current, ItemStatus<K, V, Ctx> nextStatus, Cancellable cancellable) {
        // Downgrade
        final KeyStatusPair<K, V, Ctx>[] dependencies = holder.getDependencies(current);
        Assertions.assertTrue(dependencies != null, "No dependencies for downgrade");

        AtomicReference<Ctx> contextRef = new AtomicReference<>(null);
        AtomicBoolean hasDowngraded = new AtomicBoolean(false);

        final Completable completable = Completable.defer(() -> {
                    final Ctx ctx = makeContext(holder, current, dependencies, false);
                    Assertions.assertTrue(ctx != null);
                    contextRef.setPlain(ctx);
                    return current.preDowngradeFromThis(ctx, cancellable);
                })
                .andThen(Completable.defer(() -> {
                    final boolean success = holder.setStatusDowngrade(nextStatus);
                    if (!success) {
                        cancellable.cancel();
                        return Constant.FAILED_EMPTY_COMPLETABLE;
                    }

                    hasDowngraded.setPlain(true);

                    final Ctx ctx = contextRef.getPlain();
                    Objects.requireNonNull(ctx);
                    final Completable stage = current.downgradeFromThis(ctx);
                    return stage.cache();
                }))
                .doOnEvent((throwable) -> {
                    holder.finishAction();
                    try {
                        {
                            Throwable actual = throwable;
                            while (actual instanceof CompletionException ex) actual = ex.getCause();
                            if (cancellable.isCancelled() && actual instanceof CancellationException) {
                                if (hasDowngraded.getPlain()) {
                                    holder.setStatusForDowngradeCancellation(current);
                                }
                                return;
                            }
                        }

                        final ExceptionHandlingAction action = this.tryHandleTransactionException(holder, nextStatus, false, throwable);
                        switch (action) {
                            case PROCEED -> releaseDependencies(holder, current);
                            case MARK_BROKEN -> {
                                holder.setFlag(ItemHolder.FLAG_BROKEN);
                                clearDependencies0(holder, current);
                            }
                        }
                        onItemDowngrade(holder, nextStatus);
                    } catch (Throwable t) {
                        Throwable finalT;
                        if (throwable != null) {
                            throwable.addSuppressed(t);
                            finalT = throwable;
                        } else {
                            finalT = t;
                        }
                        handleUnrecoverableException(finalT);
                    }
                });

        holder.subscribeOp(completable, this);
    }

    private void advanceStatus0(ItemHolder<K, V, Ctx, UserData> holder, ItemStatus<K, V, Ctx> nextStatus, Cancellable cancellation) {
        // Advance
        final KeyStatusPair<K, V, Ctx>[] dependencies = nextStatus.getDependencies(holder);

        Cancellable upgradeCancellable = new Cancellable();
        Cancellable depCancellable = new Cancellable();

        cancellation.setup(() -> {
            // InitAuther97: these are not interchangeable, must ensure upgrade is cancelled before dep is cancelled
            upgradeCancellable.cancel();
            depCancellable.cancel();
        });

        AtomicReference<Ctx> contextRef = new AtomicReference<>(null);

        final Completable completable = getDependencyFuture0(dependencies, holder, nextStatus, depCancellable)
                .andThen(Completable.defer(() -> {
                    final Ctx ctx = makeContext(holder, nextStatus, dependencies, true);
                    Assertions.assertTrue(ctx != null);
                    contextRef.setPlain(ctx);
                    return nextStatus.upgradeToThis(ctx, upgradeCancellable).cache();
                }))
                .onErrorResumeNext(throwable -> {
                    try {

                        {
                            Throwable actual = throwable;
                            while (actual instanceof CompletionException ex) actual = ex.getCause();
                            if (upgradeCancellable.isCancelled() && actual instanceof CancellationException) {
                                if (holder.getDependencies(nextStatus) != null) {
                                    releaseDependencies(holder, nextStatus);
                                }
                                return Completable.error(throwable);
                            }
                        }

                        Assertions.assertTrue(holder.getDependencies(nextStatus) != null);

                        final ExceptionHandlingAction action = this.tryHandleTransactionException(holder, nextStatus, true, throwable);
                        switch (action) {
                            case PROCEED -> {
                                return Completable.error(new SkipSchedulingException(throwable));
                            }
                            case MARK_BROKEN -> {
                                holder.setFlag(ItemHolder.FLAG_BROKEN);
                                clearDependencies0(holder, nextStatus);
                                return Completable.error(throwable);
                            }
                            default -> throw new IllegalStateException("Unexpected value: " + action);
                        }
                    } catch (Throwable t) {
                        if (throwable != null) {
                            throwable.addSuppressed(t);
                            return Completable.error(new SkipSchedulingException(throwable));
                        } else {
                            return Completable.error(new SkipSchedulingException(t));
                        }
                    }
                })
                .doOnEvent(throwable -> {
                    try {
                        if (throwable == null) {
                            holder.setStatusAdvance(nextStatus);
                            rerequestDependencies(holder, nextStatus);
                            onItemUpgrade(holder, nextStatus);
                        }
                    } catch (Throwable t) {
                        try {
                            holder.setFlag(ItemHolder.FLAG_BROKEN);
                            clearDependencies0(holder, nextStatus);
                        } catch (Throwable t1) {
                            t.addSuppressed(t1);
                        }
                        t.printStackTrace();
                    } finally {
                        holder.finishAction();
                    }
                })
                .andThen(
                        Completable
                                .defer(() -> {
                                    Ctx ctx = contextRef.getPlain();
                                    Assertions.assertTrue(ctx != null);
                                    return nextStatus.postUpgradeToThis(ctx).cache();
                                })
                                .onErrorResumeNext(throwable -> {
                                    final ExceptionHandlingAction action = this.tryHandleTransactionException(holder, nextStatus, true, throwable);
                                    switch (action) {
                                        case PROCEED -> {
                                        }
                                        case MARK_BROKEN -> {
                                            holder.setFlag(ItemHolder.FLAG_BROKEN);
                                            // TODO: better broken downgrade handling
                                            /*
                                            holder.executeCriticalSectionAndBusy(() -> {
                                                final var cancellation1 = Cancellable.COMPLETED;
                                                holder.submitAction(cancellation1);
                                                downgradeStatus0(holder, nextStatus, nextStatus.getPrev(), cancellation1);
                                            });
                                             */
                                        }
                                        default -> throw new IllegalStateException("Unexpected value: " + action);
                                    }
                                    return Completable.error(new SkipSchedulingException(throwable));
                                })
                )
                .cache();
        holder.subscribeOp(completable, this);
    }

    private void rerequestDependencies(ItemHolder<K, V, Ctx, UserData> holder, ItemStatus<K, V, Ctx> status) { // sync externally
        final KeyStatusPair<K, V, Ctx>[] curDep = holder.getDependencies(status);
        final KeyStatusPair<K, V, Ctx>[] newDep = status.getDependencies(holder);
        final KeyStatusPair<K, V, Ctx>[] toAdd = status.getDependenciesToAdd(holder);
        final KeyStatusPair<K, V, Ctx>[] toRemove = status.getDependenciesToRemove(holder);
        holder.setDependencies(status, null);
        holder.setDependencies(status, newDep);
        if (toAdd.length > 0) {
            ItemTicket ticket = new ItemTicket(ItemTicket.TicketType.DEPENDENCY, holder.getKey(), null, toAdd.length);
            for (KeyStatusPair<K, V, Ctx> pair : toAdd) {
                holder.addDependencyTicket(this, pair.key(), pair.status(), ticket);
            }
        }
        for (KeyStatusPair<K, V, Ctx> pair : toRemove) {
            holder.removeDependencyTicket(pair.key(), pair.status());
        }
    }

    public ItemHolder<K, V, Ctx, UserData> getHolder(K key) {
        long stamp = this.itemsLock.tryOptimisticRead();
        if (stamp != 0L) {
            try {
                ItemHolder<K, V, Ctx, UserData> holder = this.items.get(key);
                if (this.itemsLock.validate(stamp)) {
                    return holder == null || holder.isOpen() ? holder : null;
                }
                // fall through
            } catch (Throwable ignored) {
                // fall through
            }
        }

        stamp = this.itemsLock.readLock();
        try {
            final var holder = this.items.get(key);
            return holder == null || holder.isOpen() ? holder : null;
        } finally {
            this.itemsLock.unlockRead(stamp);
        }
    }

    private ItemHolder<K, V, Ctx, UserData> getOrCreateHolder(K key) {
        long stamp = this.itemsLock.tryOptimisticRead();
        ItemHolder<K, V, Ctx, UserData> holder = null;
        boolean tryReadAgain = true;
        if (stamp != 0L) {
            try {
                holder = this.items.get(key);
                if (this.itemsLock.validate(stamp)) {
                    tryReadAgain = false;
                    if (holder != null && holder.isOpen()) {
                        return holder;
                    }
                }
                // fall through
            } catch (Throwable ignored) {
                // fall through
            }
        }

        long writeStamp;
        final ItemHolder<K, V, Ctx, UserData> holder2;
        if (tryReadAgain) {
            stamp = this.itemsLock.readLock();
            try {
                holder = this.items.get(key);
            } catch (Throwable t) {
                t.printStackTrace();
                this.itemsLock.unlockRead(stamp);
                throw t;
            }
            if (holder != null && holder.isOpen()) {
                this.itemsLock.unlockRead(stamp);
                return holder;
            }
            holder2 = createHolder0(key); // move creation out of write lock region
            writeStamp = this.itemsLock.tryConvertToWriteLock(stamp);
            if (writeStamp == 0L) {
                this.itemsLock.unlockRead(stamp);
                writeStamp = this.itemsLock.writeLock();
            }
        } else {
            holder2 = createHolder0(key); // move creation out of write lock region
            writeStamp = this.itemsLock.writeLock();
        }

        // InitAuther97: ItemHolder object initialization is guarded by itemsLock,
        // make sure to use proper synchronization before changing critical sections
        final ItemHolder<K, V, Ctx, UserData> result;
        try {
            ItemHolder<K, V, Ctx, UserData> inMap = this.items.get(key);
            if (inMap == holder || inMap == null) { // put successfully
                this.onItemCreation(holder2);
                this.items.put(key, holder2);
                result = holder2;
            } else {
                result = inMap; // return the correct thing
            }
        } finally {
            this.itemsLock.unlockWrite(writeStamp);
        }
        return result;
    }

    public int itemCount() {
        this.itemsLock.tryOptimisticRead();
        return this.items.size();
    }

    private Completable getDependencyFuture0(KeyStatusPair<K, V, Ctx>[] dependencies, ItemHolder<K, V, Ctx, UserData> holder, ItemStatus<K, V, Ctx> nextStatus, Cancellable cancellable) {
        final int size = dependencies.length;
        if (size == 0) {
            cancellable.setup(Constant.NO_OP);
            cancellable.complete();
            holder.setDependencies(nextStatus, dependencies);
            return Completable.complete();
        }

        return Completable.create(emitter -> {
            AtomicInteger finished = new AtomicInteger(0);
            holder.setDependencies(nextStatus, dependencies);
            cancellable.setup(() -> {
                if (0 == finished.getAndSet(-1)) {
                    releaseDependencies(holder, nextStatus);
                    scheduleFlushDependencyCache(holder); // avoid dep cache poison due to partial upgrades when cancelled
                    emitter.onError(Constant.CANCELLED);
                }
            });
            try {
                Runnable callback = () -> {
                    final int res = finished.getAndSet(1);
                    Assertions.assertTrue(res != 1, "Multiple consumption of callback");
                    if (res != 0) {
                        return;
                    }
                    cancellable.complete();
                    holder.getCriticalSectionExecutor().execute(emitter::onComplete);
                };
                final ItemTicket ticket = new ItemTicket(ItemTicket.TicketType.DEPENDENCY, holder.getKey(), callback, dependencies.length);
                for (KeyStatusPair<K, V, Ctx> dependency : dependencies) {
                    Assertions.assertTrue(!dependency.key().equals(holder.getKey()));
                    holder.addDependencyTicket(this, dependency.key(), dependency.status(), ticket);
                }
            } catch (Throwable t) {
                t.printStackTrace();
                if (0 == finished.getAndSet(-2)) {
                    releaseDependencies(holder, nextStatus);
                    scheduleFlushDependencyCache(holder); // avoid dep cache poison due to partial upgrades when cancelled
                    emitter.onError(t);
                }
            }
        });
    }

    public ItemHolder<K, V, Ctx, UserData> addTicket(K key, ItemStatus<K, V, Ctx> targetStatus, Runnable callback) {
        return this.addTicket(key, key, targetStatus, callback);
    }

    public ItemHolder<K, V, Ctx, UserData> addTicket(K key, Object source, ItemStatus<K, V, Ctx> targetStatus, Runnable callback) {
        return this.addTicket(key, ItemTicket.TicketType.EXTERNAL, source, targetStatus, callback);
    }

    public ItemHolder<K, V, Ctx, UserData> addTicket(K key, ItemTicket.TicketType type, Object source, ItemStatus<K, V, Ctx> targetStatus, Runnable callback) {
        return this.addTicket0(key, targetStatus, new ItemTicket(type, source, callback));
    }

    public ItemHolder<K, V, Ctx, UserData> addTicket0(K key, ItemStatus<K, V, Ctx> targetStatus, ItemTicket ticket) {
        Objects.requireNonNull(targetStatus);
        Objects.requireNonNull(ticket);
        if (this.getUnloadedStatus().equals(targetStatus)) {
            throw new IllegalArgumentException("Cannot add ticket to unloaded status");
        }
        try {
            ItemHolder<K, V, Ctx, UserData> holder;
            byte retVal;
            do {
                holder = this.getOrCreateHolder(key);
                retVal = holder.addTicket(targetStatus, ticket);
            } while (retVal == ItemHolder.R_REMOVED); // Holder is removed before we had chance to add a ticket to it, retry
            // Eliminate some useless markDirty here
            if (retVal == ItemHolder.R_MARK_DIRTY) {
                holder.markDirty(this);
            }
            return holder;
        } catch (Throwable t) {
            t.printStackTrace();
            throw new RuntimeException(t);
        }
    }

    private ItemHolder<K, V, Ctx, UserData> createHolder0(K k) {
        ItemHolder<K, V, Ctx, UserData> holder = new ItemHolder<>(this.getUnloadedStatus(), k, this.objectFactory, this.getBackgroundExecutor());
        this.onItemConstruct(holder);
        return holder;
    }

    public void removeTicket(K key, ItemStatus<K, V, Ctx> targetStatus) {
        this.removeTicket(key, ItemTicket.TicketType.EXTERNAL, key, targetStatus);
    }

    public void removeTicket(K key, ItemTicket.TicketType type, Object source, ItemStatus<K, V, Ctx> targetStatus) {
        this.removeTicket0(key, targetStatus, new ItemTicket(type, source, null));
    }

    public void removeTicket0(K key, ItemStatus<K, V, Ctx> targetStatus, ItemTicket ticket) {
        Objects.requireNonNull(targetStatus);
        Objects.requireNonNull(ticket);
        ItemHolder<K, V, Ctx, UserData> holder = this.getHolder(key);
        if (holder == null) {
            throw new IllegalStateException("No such item");
        }
        // Eliminate some useless markDirty here
        if (ItemHolder.R_MARK_DIRTY == holder.removeTicket(targetStatus, ticket)) {
            // holder may have been removed at this point, only mark it dirty if it still exists
            holder.tryMarkDirty(this);
        }
    }

    private ItemStatus<K, V, Ctx> getNextStatus(ItemStatus<K, V, Ctx> currentStatus, byte target) {
        final byte current = currentStatus.getOrdinal();
        if (current < target) {
            return currentStatus.getNext();
        } else if (current > target) {
            return currentStatus.getPrev();
        } else {
            return currentStatus;
        }
    }

    private ExceptionHandlingAction tryHandleTransactionException(ItemHolder<K, V, Ctx, UserData> holder, ItemStatus<K, V, Ctx> nextStatus, boolean isUpgrade, Throwable throwable) {
        if (throwable == null) { // no exception to handle
            return ExceptionHandlingAction.PROCEED;
        }
        try {
            return this.handleTransactionException(holder, nextStatus, isUpgrade, throwable);
        } catch (Throwable t) {
            t.printStackTrace();
            return ExceptionHandlingAction.MARK_BROKEN;
        }
    }

    private void clearDependencies0(final ItemHolder<K, V, Ctx, UserData> holder, final ItemStatus<K, V, Ctx> fromStatus) { // sync externally
        for (int i = fromStatus.getOrdinal(); i > 0; i--) {
            final ItemStatus<K, V, Ctx> status = this.getUnloadedStatus().getAllStatuses()[i];
            this.releaseDependencies(holder, status);
            holder.setDependencies(status, ItemStatus.emptyDependencies());
        }
    }

    private void releaseDependencies(ItemHolder<K, V, Ctx, UserData> holder, ItemStatus<K, V, Ctx> status) {
        final KeyStatusPair<K, V, Ctx>[] dependencies = holder.getDependencies(status);
        for (KeyStatusPair<K, V, Ctx> dependency : dependencies) {
            holder.removeDependencyTicket(dependency.key(), dependency.status());
        }
        holder.setDependencies(status, null);
    }

}
