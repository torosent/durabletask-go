package client

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// pagedQueryServer serves a deterministic, unbounded stream of single-instance
// query pages so the client-side tag filter scan cap can be observed exactly.
type pagedQueryServer struct {
	protos.UnimplementedTaskHubSidecarServiceServer

	// matchEvery makes every Nth served instance carry the searched tag. Zero
	// means no instance ever matches.
	matchEvery int
	// totalPages bounds the stream. Zero means the stream never ends.
	totalPages int

	mu       sync.Mutex
	requests []string
}

func (s *pagedQueryServer) QueryInstances(
	_ context.Context,
	req *protos.QueryInstancesRequest,
) (*protos.QueryInstancesResponse, error) {
	token := req.GetQuery().GetContinuationToken().GetValue()
	s.mu.Lock()
	s.requests = append(s.requests, token)
	served := len(s.requests)
	s.mu.Unlock()

	tags := map[string]string{"group": "other"}
	if s.matchEvery > 0 && served%s.matchEvery == 0 {
		tags = map[string]string{"group": "wanted"}
	}
	resp := &protos.QueryInstancesResponse{
		OrchestrationState: []*protos.OrchestrationState{{
			InstanceId:          fmt.Sprintf("instance-%04d", served),
			Name:                "Paged",
			OrchestrationStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
			Tags:                tags,
		}},
	}
	if s.totalPages == 0 || served < s.totalPages {
		resp.ContinuationToken = wrapperspb.String(fmt.Sprintf("token-%04d", served))
	}
	return resp, nil
}

func (s *pagedQueryServer) requestTokens() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.requests...)
}

func startQueryClient(t *testing.T, server protos.TaskHubSidecarServiceServer) *TaskHubGrpcClient {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	protos.RegisterTaskHubSidecarServiceServer(grpcServer, server)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	t.Cleanup(grpcServer.Stop)

	connection, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, connection.Close()) })
	return NewTaskHubGrpcClient(connection, backend.DefaultLogger())
}

// TestQueryInstancesTagFilterHonoursScanPageCap asserts the documented remote
// tag filter contract: because the wire query has no tag predicate, the client
// scans at most api.MaxRemoteTagFilterScanPages service pages per call and then
// returns a short page plus a continuation token the caller can resume from.
func TestQueryInstancesTagFilterHonoursScanPageCap(t *testing.T) {
	for _, test := range []struct {
		name             string
		matchEvery       int
		pageSize         int
		wantMatches      int
		wantServedPages  int
		wantTokenPresent bool
	}{
		{
			name:             "no-matches-stops-at-cap",
			pageSize:         5,
			wantMatches:      0,
			wantServedPages:  api.MaxRemoteTagFilterScanPages,
			wantTokenPresent: true,
		},
		{
			name:             "sparse-matches-stop-at-cap",
			matchEvery:       50,
			pageSize:         5,
			wantMatches:      2,
			wantServedPages:  api.MaxRemoteTagFilterScanPages,
			wantTokenPresent: true,
		},
		{
			name:             "dense-matches-fill-the-page-before-the-cap",
			matchEvery:       1,
			pageSize:         5,
			wantMatches:      5,
			wantServedPages:  5,
			wantTokenPresent: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := &pagedQueryServer{matchEvery: test.matchEvery}
			client := startQueryClient(t, server)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			result, err := client.QueryInstances(ctx, api.OrchestrationQuery{
				PageSize: test.pageSize,
				Tags:     map[string]string{"group": "wanted"},
			})
			require.NoError(t, err)
			require.Len(t, result.Orchestrations, test.wantMatches)
			require.Len(t, server.requestTokens(), test.wantServedPages)
			require.Equal(t, test.wantTokenPresent, result.ContinuationToken != "")
		})
	}
}

// TestQueryInstancesTagFilterResumesAfterScanPageCap asserts the continuation
// token returned by a capped scan resumes exactly where the previous scan
// stopped, so callers can keep paging without losing or repeating instances.
func TestQueryInstancesTagFilterResumesAfterScanPageCap(t *testing.T) {
	server := &pagedQueryServer{}
	client := startQueryClient(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	query := api.OrchestrationQuery{PageSize: 5, Tags: map[string]string{"group": "wanted"}}
	first, err := client.QueryInstances(ctx, query)
	require.NoError(t, err)
	require.Empty(t, first.Orchestrations)
	require.Equal(t, fmt.Sprintf("token-%04d", api.MaxRemoteTagFilterScanPages), first.ContinuationToken)

	query.ContinuationToken = first.ContinuationToken
	second, err := client.QueryInstances(ctx, query)
	require.NoError(t, err)
	require.Empty(t, second.Orchestrations)
	require.Equal(t, fmt.Sprintf("token-%04d", 2*api.MaxRemoteTagFilterScanPages), second.ContinuationToken)

	tokens := server.requestTokens()
	require.Len(t, tokens, 2*api.MaxRemoteTagFilterScanPages)
	// The resumed scan starts from the token the capped scan handed back.
	require.Equal(t, "", tokens[0])
	require.Equal(t, first.ContinuationToken, tokens[api.MaxRemoteTagFilterScanPages])
}

// TestQueryInstancesWithoutTagsIgnoresScanPageCap asserts the scan cap only
// exists to bound client-side tag filtering: an untagged query pages until the
// requested page size is filled.
func TestQueryInstancesWithoutTagsIgnoresScanPageCap(t *testing.T) {
	server := &pagedQueryServer{}
	client := startQueryClient(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pageSize := api.MaxRemoteTagFilterScanPages + 7
	result, err := client.QueryInstances(ctx, api.OrchestrationQuery{PageSize: pageSize})
	require.NoError(t, err)
	require.Len(t, result.Orchestrations, pageSize)
	require.Len(t, server.requestTokens(), pageSize)
	require.NotEmpty(t, result.ContinuationToken)
}

// TestQueryInstancesTagFilterStopsWhenServiceExhausted asserts a capped tag scan
// that reaches the end of the service results returns no continuation token.
func TestQueryInstancesTagFilterStopsWhenServiceExhausted(t *testing.T) {
	server := &pagedQueryServer{matchEvery: 3, totalPages: 10}
	client := startQueryClient(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := client.QueryInstances(ctx, api.OrchestrationQuery{
		PageSize: 50,
		Tags:     map[string]string{"group": "wanted"},
	})
	require.NoError(t, err)
	require.Len(t, result.Orchestrations, 3)
	require.Empty(t, result.ContinuationToken)
	require.Len(t, server.requestTokens(), 10)
}

// nonAdvancingQueryServer always echoes the same continuation token, which the
// client must reject instead of looping forever.
type nonAdvancingQueryServer struct {
	protos.UnimplementedTaskHubSidecarServiceServer
}

func (*nonAdvancingQueryServer) QueryInstances(
	_ context.Context,
	req *protos.QueryInstancesRequest,
) (*protos.QueryInstancesResponse, error) {
	token := req.GetQuery().GetContinuationToken().GetValue()
	if token == "" {
		token = "stuck"
	}
	return &protos.QueryInstancesResponse{ContinuationToken: wrapperspb.String(token)}, nil
}

func TestQueryInstancesRejectsNonAdvancingContinuationToken(t *testing.T) {
	client := startQueryClient(t, &nonAdvancingQueryServer{})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := client.QueryInstances(ctx, api.OrchestrationQuery{
		PageSize:          5,
		ContinuationToken: "stuck",
	})
	require.ErrorContains(t, err, "non-advancing continuation token")
}
