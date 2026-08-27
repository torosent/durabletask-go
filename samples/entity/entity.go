// This sample demonstrates how to use durable entities with the Durable Task Go SDK.
// It shows two patterns:
//  1. A raw entity function (Counter) with manual operation dispatch
//  2. An auto-dispatch entity (BankAccount) where operations map to methods on a struct
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/backend/sqlite"
	"github.com/microsoft/durabletask-go/task"
)

func main() {
	r := task.NewTaskRegistry()

	// Pattern 1: Register a raw entity function with manual dispatch
	if err := r.AddEntityN("counter", CounterEntity); err != nil {
		log.Fatalf("Failed to register counter entity: %v", err)
	}

	// Pattern 2: Register an auto-dispatch entity backed by a struct
	if err := r.AddEntityN("bankaccount", task.NewEntityFor[BankAccount]()); err != nil {
		log.Fatalf("Failed to register bank account entity: %v", err)
	}
	if err := r.AddOrchestratorN("transfer", TransferOrchestrator); err != nil {
		log.Fatalf("Failed to register transfer orchestrator: %v", err)
	}

	ctx := context.Background()
	client, worker, err := Init(ctx, r)
	if err != nil {
		log.Fatalf("Failed to initialize: %v", err)
	}
	defer func() {
		if err := worker.Shutdown(ctx); err != nil {
			log.Printf("Failed to shutdown: %v", err)
		}
	}()

	// --- Demo 1: Counter entity (raw function) ---
	fmt.Println("=== Counter Entity Demo ===")
	counterID := api.NewEntityID("counter", "myCounter")

	// Signal the entity to perform operations
	if err := client.SignalEntity(ctx, counterID, "add", api.WithSignalInput(10)); err != nil {
		log.Printf("Failed to signal entity: %v", err) //nolint:gocritic // sample code, keeping simple
		return
	}
	if err := client.SignalEntity(ctx, counterID, "add", api.WithSignalInput(5)); err != nil {
		log.Printf("Failed to signal entity: %v", err)
		return
	}
	if err := client.SignalEntity(ctx, counterID, "add", api.WithSignalInput(-3)); err != nil {
		log.Printf("Failed to signal entity: %v", err)
		return
	}

	// Query the entity state
	meta, err := waitForEntityState(ctx, client, counterID, "12")
	if err != nil {
		log.Printf("Failed to fetch entity: %v", err)
		return
	}
	fmt.Printf("Counter state: %s\n", meta.SerializedState) // Expected: 12

	// --- Demo 2: BankAccount entity (auto-dispatch) ---
	fmt.Println("\n=== Bank Account Entity Demo ===")
	accountID := api.NewEntityID("bankaccount", "checking-001")

	if err := client.SignalEntity(ctx, accountID, "Deposit", api.WithSignalInput(1000)); err != nil {
		log.Printf("Failed to signal entity: %v", err)
		return
	}
	if err := client.SignalEntity(ctx, accountID, "Deposit", api.WithSignalInput(500)); err != nil {
		log.Printf("Failed to signal entity: %v", err)
		return
	}
	if err := client.SignalEntity(ctx, accountID, "Withdraw", api.WithSignalInput(200)); err != nil {
		log.Printf("Failed to signal entity: %v", err)
		return
	}

	meta, err = waitForEntityState(ctx, client, accountID, `{"balance":1300}`)
	if err != nil {
		log.Printf("Failed to fetch entity: %v", err)
		return
	}
	fmt.Printf("Bank account state: %s\n", meta.SerializedState) // Expected: {"balance":1300}

	savingsID := api.NewEntityID("bankaccount", "savings-001")
	if err := client.SignalEntity(ctx, savingsID, "Deposit", api.WithSignalInput(100)); err != nil {
		log.Printf("Failed to initialize savings account: %v", err)
		return
	}
	if _, err := waitForEntityState(ctx, client, savingsID, `{"balance":100}`); err != nil {
		log.Printf("Failed to initialize savings account: %v", err)
		return
	}
	transferID, err := client.ScheduleNewOrchestration(
		ctx,
		"transfer",
		api.WithInput(TransferInput{From: accountID, To: savingsID, Amount: 300}),
	)
	if err != nil {
		log.Printf("Failed to schedule transfer: %v", err)
		return
	}
	transfer, err := client.WaitForOrchestrationCompletion(ctx, transferID)
	if err != nil {
		log.Printf("Transfer failed: %v", err)
		return
	}
	fmt.Printf("Transfer result: %s\n", transfer.SerializedOutput)

	fmt.Println("\nDone!")
}

// Init creates and initializes an in-memory client and worker pair.
func Init(ctx context.Context, r *task.TaskRegistry) (backend.EntityTaskHubClient, backend.TaskHubWorker, error) {
	logger := backend.DefaultLogger()
	be := sqlite.NewSqliteBackend(sqlite.NewSqliteOptions(""), logger)
	executor := task.NewTaskExecutor(r)
	orchestrationWorker := backend.NewOrchestrationWorker(be, executor, logger)
	activityWorker := backend.NewActivityTaskWorker(be, executor, logger)
	entityBackend, ok := backend.GetBackendCapability[backend.EntityBackend](be)
	if !ok {
		return nil, nil, fmt.Errorf("backend does not support durable entities")
	}
	entityWorker := backend.NewEntityWorker(
		entityBackend,
		executor.(backend.EntityExecutor),
		logger,
	)
	taskHubWorker := backend.NewTaskHubWorker(be, orchestrationWorker, activityWorker, logger, entityWorker)
	if err := taskHubWorker.Start(ctx); err != nil {
		return nil, nil, err
	}
	taskHubClient := backend.NewTaskHubClient(be)
	return taskHubClient.(backend.EntityTaskHubClient), taskHubWorker, nil
}

// --- Pattern 1: Raw entity function ---

// CounterEntity is a simple counter entity that supports "add", "get", and "reset" operations.
func CounterEntity(ctx *task.EntityContext) (any, error) {
	var count int
	if ctx.HasState() {
		if err := ctx.GetState(&count); err != nil {
			return nil, err
		}
	}

	switch ctx.Operation {
	case "add":
		var amount int
		if err := ctx.GetInput(&amount); err != nil {
			return nil, err
		}
		count += amount
	case "get":
		// just return current value
	case "reset":
		count = 0
	default:
		return nil, fmt.Errorf("unknown operation: %s", ctx.Operation)
	}

	if err := ctx.SetState(count); err != nil {
		return nil, err
	}
	return count, nil
}

// --- Pattern 2: Auto-dispatch entity ---

// BankAccount is a struct-based entity. Public methods are automatically
// dispatched by operation name (case-insensitive).
type BankAccount struct {
	Balance int `json:"balance"`
}

func (a *BankAccount) Deposit(amount int) (any, error) {
	a.Balance += amount
	return a.Balance, nil
}

func (a *BankAccount) Withdraw(amount int) (any, error) {
	if amount > a.Balance {
		return nil, fmt.Errorf("insufficient funds: balance=%d, withdrawal=%d", a.Balance, amount)
	}
	a.Balance -= amount
	return a.Balance, nil
}

func (a *BankAccount) Get() (any, error) {
	return a.Balance, nil
}

type TransferInput struct {
	From   api.EntityID `json:"from"`
	To     api.EntityID `json:"to"`
	Amount int          `json:"amount"`
}

func TransferOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var input TransferInput
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	unlock, err := ctx.LockEntities(input.From, input.To)
	if err != nil {
		return nil, err
	}
	defer unlock()

	var fromBalance int
	if err := ctx.CallEntity(
		input.From,
		"Withdraw",
		task.WithEntityInput(input.Amount),
	).Await(&fromBalance); err != nil {
		return nil, err
	}
	var toBalance int
	if err := ctx.CallEntity(
		input.To,
		"Deposit",
		task.WithEntityInput(input.Amount),
	).Await(&toBalance); err != nil {
		return nil, err
	}
	return map[string]int{"from": fromBalance, "to": toBalance}, nil
}

func waitForEntityState(
	ctx context.Context,
	client backend.EntityTaskHubClient,
	entityID api.EntityID,
	expected string,
) (*api.EntityMetadata, error) {
	timeout := time.NewTimer(10 * time.Second)
	defer timeout.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		metadata, err := client.FetchEntityMetadata(ctx, entityID, true)
		if err != nil {
			return nil, err
		}
		if metadata != nil && metadata.SerializedState == expected {
			return metadata, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timeout.C:
			return nil, fmt.Errorf("timed out waiting for %s state %s", entityID.String(), expected)
		case <-ticker.C:
		}
	}
}
