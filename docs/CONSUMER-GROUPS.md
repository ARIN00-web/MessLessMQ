# Consumer groups

The `GROUP` API v1 is carried by the normal versioned binary TCP frame. Its payload has an `operation` field:

- `join`: registers a member and returns generation plus eager assignments.
- `heartbeat`: renews the member lease for its generation.
- `leave`: removes a member and triggers a rebalance.
- `fetch`: reads only a partition assigned to that member/generation.
- `commit_offset`: durably commits the next unread offset for an owned partition.
- `fetch_offset`: returns the persisted offset for a group partition.

Membership is local to a broker process today. The broker expires members after 30 seconds without a heartbeat and restores durable offsets from `__consumer_offsets` on assignment. A client must rejoin after a stale-generation response and must treat a commit error after a network failure as potentially uncertain.

Group coordination is not yet controller-replicated, so coordinator-broker loss requires members to rejoin. Do not use this API for cross-broker exactly-once processing.
