package dev.hotaru.event;

/**
 * Context passed to transactional event dispatch, enabling Saga compensations and commit hooks.
 */
public interface TransactionContext {

    /**
     * Registers a compensation action that will be executed in reverse order if the transaction rolls back.
     *
     * @param compensationAction the rollback action
     */
    void onRollback(Runnable compensationAction);

    /**
     * Registers a hook that will be executed after the transaction commits successfully.
     *
     * @param commitHook the commit hook
     */
    void onCommit(Runnable commitHook);

    /**
     * Explicitly marks the transaction as rollback-only.
     */
    void rollback();

    /**
     * Checks whether the current transaction has been marked for rollback.
     */
    boolean isRollbackOnly();
}
