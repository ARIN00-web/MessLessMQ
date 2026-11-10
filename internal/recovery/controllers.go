package recovery

import (
	"fmt"

	"github.com/ARIN00-web/messlessmq/internal/controller"
)

// RecoverController re-enables one controller and synchronizes it from the
// current metadata leader. If no leader exists, elect one first with
// RecoverControllerCluster or controller.Cluster.Campaign.
func RecoverController(cluster *controller.Cluster, nodeID string) error {
	if cluster == nil || nodeID == "" {
		return ErrInvalidConfig
	}
	if err := cluster.SetAvailable(nodeID, true); err != nil {
		return err
	}
	leaderID := cluster.CurrentLeader()
	if leaderID == "" {
		return ErrNoControllerLeader
	}
	if leaderID == nodeID {
		return nil
	}
	return cluster.SyncFollower(nodeID)
}

// ElectAndRecoverControllers elects the preferred candidate when the cluster
// has no active leader, then synchronizes each supplied recovered follower.
// The caller must explicitly list nodes it intends to bring back online.
func ElectAndRecoverControllers(cluster *controller.Cluster, candidateID string, followerIDs []string) error {
	if cluster == nil || candidateID == "" {
		return ErrInvalidConfig
	}
	if err := cluster.SetAvailable(candidateID, true); err != nil {
		return err
	}
	if cluster.CurrentLeader() == "" {
		if err := cluster.Campaign(candidateID); err != nil {
			return fmt.Errorf("elect metadata controller: %w", err)
		}
	}
	leaderID := cluster.CurrentLeader()
	toRecover := append([]string(nil), followerIDs...)
	if candidateID != leaderID {
		found := false
		for _, id := range toRecover {
			if id == candidateID {
				found = true
				break
			}
		}
		if !found {
			toRecover = append(toRecover, candidateID)
		}
	}
	seen := make(map[string]struct{}, len(toRecover))
	for _, followerID := range toRecover {
		if followerID == leaderID {
			continue
		}
		if _, duplicate := seen[followerID]; duplicate {
			continue
		}
		seen[followerID] = struct{}{}
		if err := RecoverController(cluster, followerID); err != nil {
			return fmt.Errorf("recover controller %q: %w", followerID, err)
		}
	}
	return nil
}
