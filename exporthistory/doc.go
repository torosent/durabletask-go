// Package exporthistory exports terminal orchestration histories to Azure Blob
// Storage using durable entities, orchestrations, and activities.
//
// # Maturity: preview
//
// This package is a preview API. Its exported types, function signatures, the
// serialized shape of the ExportJob entity state, and the names of the system
// entity, orchestrators, and activities may change in a future release without a
// major version bump. Do not depend on it for production workloads that cannot
// tolerate a breaking change, and drain in-flight export jobs before upgrading.
//
// # Model
//
// An export job is a durable entity keyed by job ID. Creating a job transitions
// the entity to [ExportJobStatusActive] and signals it to start a dedicated
// export orchestration whose instance ID is derived from the job ID, so a job
// never runs two orchestrations concurrently. The orchestration repeatedly lists
// terminal orchestration instances that match the job's filter, exports each
// instance's history, and commits a checkpoint back to the entity. A batch job
// completes when a listing page comes back empty; a continuous job idles and
// polls again.
//
// # Usage
//
// Workers register the system tasks and advertise them to the service:
//
//	registry := task.NewTaskRegistry()
//	store, err := exporthistory.NewAzureBlobStore(exporthistory.AzureBlobStoreOptions{
//		ConnectionString: connectionString,
//		ContainerName:    "history-exports",
//	})
//	if err != nil {
//		return err
//	}
//	if err := exporthistory.Register(registry, exporthistory.WorkerOptions{
//		Source: taskHubClient,
//		Store:  store,
//	}); err != nil {
//		return err
//	}
//	worker, err := durabletaskscheduler.NewWorker(
//		options, registry, logger, exporthistory.WithExportHistory())
//
// Clients create and inspect jobs:
//
//	exportClient, err := exporthistory.NewClient(taskHubClient, exporthistory.ClientOptions{
//		ContainerName: "history-exports",
//	})
//	if err != nil {
//		return err
//	}
//	job, err := exportClient.CreateJob(ctx, exporthistory.JobCreationOptions{
//		Mode:              exporthistory.ExportModeBatch,
//		CompletedTimeFrom: from,
//		CompletedTimeTo:   to,
//	})
//	if err != nil {
//		return err
//	}
//	description, err := job.Describe(ctx)
//
// # Limitations
//
// Export requires a task hub that implements the instance-ID listing and history
// streaming management APIs. Extended sessions are not supported.
package exporthistory
