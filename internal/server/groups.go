package server

import (
	"context"
	"time"
)

const (
	groupSessionTimeout = 30 * time.Second
	groupExpiryInterval = time.Second
)

// runGroupExpiration gives the integrated coordinator bounded member leases.
// Membership itself remains in-memory; durable offsets are restored whenever a
// rebalance assigns a partition to a joining member.
func (s *Server) runGroupExpiration(ctx context.Context) {
	ticker := time.NewTicker(groupExpiryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if _, err := s.groupCoordinator.ExpireMembers(now, groupSessionTimeout); err != nil {
				s.logger.Warn("expire consumer group members", "error", err)
			}
		}
	}
}
