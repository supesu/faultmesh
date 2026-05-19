package stream

import (
	"context"

	"github.com/rs/zerolog"

	pb "github.com/supesu/faultmesh/data-plane/pkg/genproto/faultmesh/v1"
)

type ActionHandler interface {
	HandleAction(ctx context.Context, req *pb.ActionRequest) error
}

type noopHandler struct{ logger zerolog.Logger }

func NewNoopHandler(logger zerolog.Logger) ActionHandler {
	return &noopHandler{logger: logger}
}

func (h *noopHandler) HandleAction(_ context.Context, req *pb.ActionRequest) error {
	h.logger.Info().
		Str("action_id", req.GetActionId()).
		Msg("received action (no-op handler)")
	return nil
}
