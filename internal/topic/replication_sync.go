package topic

// AppendAtSync appends a timestamped record and synchronizes the log and its
// indexes before returning. Replication acknowledgements use this operation
// so an acknowledged follower has persisted the replicated record.
func (p *Partition) AppendAtSync(data []byte, timestamp int64) (uint64, error) {
	log, err := p.storageLog()
	if err != nil {
		return 0, err
	}

	offset, err := log.AppendAt(data, timestamp)
	if err != nil {
		return 0, err
	}

	if err := log.Sync(); err != nil {
		return offset, err
	}

	return offset, nil
}
