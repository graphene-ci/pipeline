// Package leak is how a machine record says it could not clean up after
// itself. A record whose finalize gives up — the daemon stayed away, the
// file would not go — still lets the record go, so one stuck removal never
// wedges a cascade; Report writes the leftover onto the machine's agent
// record instead of only into a log. A machine with leaks is dirty: the
// agent record lists them until an operator clears them.
package leak

import (
	"errors"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/wire"
)

// Report records that the calling record's removal gave up with cause. It
// runs in the record's workflow (a finalize), on the queue of the machine
// the record lives on; a record not hosted on a machine has nothing to
// report and only logs. Reporting is bounded and best effort itself: a
// server that cannot be reached does not hold the cascade.
func Report(ctx workflow.Context, cause error) {
	info := workflow.GetInfo(ctx)
	ref := info.WorkflowExecution.ID
	log := workflow.GetLogger(ctx)
	agent, _, onMachine := wire.ParseAgentRunQueue(info.TaskQueueName)
	if !onMachine {
		log.Warn("removal gave up; the record is not on a machine", "record", ref, "error", cause)
		return
	}
	if cause == nil {
		cause = errors.New("removal gave up")
	}
	actx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:              wire.ServerQueue,
		StartToCloseTimeout:    30 * time.Second,
		ScheduleToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    30 * time.Second,
		},
	})
	req := wire.ReportLeakRequest{Error: cause.Error()}
	if err := workflow.ExecuteActivity(actx, wire.ReportLeakActivity, req).Get(ctx, nil); err != nil {
		log.Error("leak not recorded on the machine", "record", ref, "agent", string(agent), "error", err, "leak", cause)
		return
	}
	log.Warn("removal gave up; leak recorded on the machine", "record", ref, "agent", string(agent), "error", cause)
}
