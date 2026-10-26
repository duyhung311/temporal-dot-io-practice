// @@@SNIPSTART money-transfer-java-tests
package moneytransferapp;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowFailedException;
import io.temporal.client.WorkflowStub;
import io.temporal.failure.ApplicationFailure;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;

public class MoneyTransferWorkflowTest {

    private TestWorkflowEnvironment testEnv;
    private Worker worker;
    private WorkflowClient workflowClient;

    @Before
    public void setUp() {
        testEnv = TestWorkflowEnvironment.newInstance();
        worker = testEnv.newWorker(Shared.MONEY_TRANSFER_TASK_QUEUE);
        worker.registerWorkflowImplementationTypes(MoneyTransferWorkflowImpl.class);
        workflowClient = testEnv.getWorkflowClient();
    }

    @After
    public void tearDown() {
        testEnv.close();
    }

    @Test
    public void testTransfer() {
        AccountActivity activities = mock(AccountActivityImpl.class);
        worker.registerActivitiesImplementations(activities);
        testEnv.start();
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setTaskQueue(Shared.MONEY_TRANSFER_TASK_QUEUE)
                .build();
        MoneyTransferWorkflow workflow = workflowClient.newWorkflowStub(MoneyTransferWorkflow.class, options);
        TransactionDetails transaction = new CoreTransactionDetails("account1", "account2", "reference1", 10);
        workflow.transfer(transaction);
        verify(activities).withdraw(eq("account1"), eq("reference1"), eq(10));
        // Cannot reliably test deposit as the failure configurations will affect this
    }

    private MoneyTransferWorkflow newWorkflow() {
        return workflowClient.newWorkflowStub(
                MoneyTransferWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setTaskQueue(Shared.MONEY_TRANSFER_TASK_QUEUE)
                        .build());
    }

    @Test
    public void testTransferOver50WaitsForApprovalThenCompletes() {
        // Spy on the real implementation so requireApproval() uses the real "> 50" rule
        AccountActivity activities = spy(new AccountActivityImpl());
        worker.registerActivitiesImplementations(activities);
        testEnv.start();

        MoneyTransferWorkflow workflow = newWorkflow();
        TransactionDetails transaction = new CoreTransactionDetails("account1", "account2", "reference1", 100);

        // Start asynchronously so the test can send the signal while the workflow waits
        WorkflowClient.start(workflow::transfer, transaction);

        // Move time forward 1 minute: the workflow should still be waiting, nothing withdrawn
        testEnv.sleep(Duration.ofMinutes(1));
        verify(activities).requireApproval(100);
        verify(activities, never()).withdraw(anyString(), anyString(), anyInt());

        // Approve and wait for the workflow to finish
        workflow.approveOrder();
        WorkflowStub.fromTyped(workflow).getResult(Void.class);

        verify(activities).withdraw(eq("account1"), eq("reference1"), eq(100));
        verify(activities).deposit(eq("account2"), eq("reference1"), eq(100));
    }

    @Test
    public void testTransferOver50FailsWithoutApproval() {
        AccountActivity activities = spy(new AccountActivityImpl());
        worker.registerActivitiesImplementations(activities);
        testEnv.start();

        MoneyTransferWorkflow workflow = newWorkflow();
        TransactionDetails transaction = new CoreTransactionDetails("account1", "account2", "reference1", 100);
        WorkflowClient.start(workflow::transfer, transaction);

        // No signal sent. The test server skips ahead past the 5-minute timeout automatically.
        try {
            WorkflowStub.fromTyped(workflow).getResult(Void.class);
            fail("Expected the workflow to fail without approval");
        } catch (WorkflowFailedException e) {
            ApplicationFailure cause = (ApplicationFailure) e.getCause();
            assertEquals("ApprovalTimeout", cause.getType());
        }

        verify(activities, never()).withdraw(anyString(), anyString(), anyInt());
    }

    @Test
    public void testTransferOf50DoesNotNeedApproval() {
        AccountActivity activities = spy(new AccountActivityImpl());
        worker.registerActivitiesImplementations(activities);
        testEnv.start();

        MoneyTransferWorkflow workflow = newWorkflow();
        // Runs synchronously: no signal needed, so it completes on its own
        workflow.transfer(new CoreTransactionDetails("account1", "account2", "reference1", 50));

        verify(activities).requireApproval(50);
        verify(activities).withdraw(eq("account1"), eq("reference1"), eq(50));
    }

    @Test
    public void testRequireApprovalRule() {
        AccountActivityImpl impl = new AccountActivityImpl();
        assertFalse(impl.requireApproval(50));
        assertTrue(impl.requireApproval(51));
    }
}
// @@@SNIPEND
