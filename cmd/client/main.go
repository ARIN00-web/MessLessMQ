package main

import (
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"net"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/ARIN00-web/messlessmq/internal/network"
	"github.com/ARIN00-web/messlessmq/internal/protocol"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "produce":
		err = runProduce(os.Args[2:])
	case "consume":
		err = runConsume(os.Args[2:])
	case "metadata":
		err = runMetadata(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "messlessmq-client:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "Usage: messlessmq-client <produce|consume|metadata> [flags]")
}

func parseFlags(name string, args []string, setup func(*flag.FlagSet) error) error {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	if err := setup(fs); err != nil {
		return err
	}
	return fs.Parse(args)
}

type brokerResponseError struct {
	Code uint16
}

func (e *brokerResponseError) Error() string {
	return fmt.Sprintf("broker returned error code %d", e.Code)
}

func request(address string, api uint16, version uint16, payload []byte) (protocol.Response, error) {
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		return protocol.Response{}, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	frame, err := protocol.EncodeRequest(protocol.Request{APIKey: api, APIVersion: version, CorrelationID: uint32(time.Now().UnixNano()), Payload: payload})
	if err != nil {
		return protocol.Response{}, err
	}
	if err := network.WriteFrame(conn, frame); err != nil {
		return protocol.Response{}, err
	}
	responseFrame, err := network.ReadFrame(conn)
	if err != nil {
		return protocol.Response{}, err
	}
	response, err := protocol.DecodeResponse(responseFrame)
	if err != nil {
		return protocol.Response{}, err
	}
	if response.ErrorCode != protocol.ErrorNone {
		return response, &brokerResponseError{Code: response.ErrorCode}
	}
	return response, nil
}

func fetchMetadata(address, topicName string) (protocol.MetadataResponse, error) {
	payload, err := protocol.EncodeMetadataRequest(protocol.MetadataRequest{Topic: topicName})
	if err != nil {
		return protocol.MetadataResponse{}, err
	}
	response, err := request(address, protocol.APIMETADATA, protocol.MetadataAPIVersion, payload)
	if err != nil {
		return protocol.MetadataResponse{}, err
	}
	return protocol.DecodeMetadataResponse(response.Payload)
}

// routeProduceAddress mirrors the broker's FNV-1a key partitioner and resolves
// the currently committed partition leader from a metadata snapshot.
func partitionForKey(metadata protocol.MetadataResponse, topicName string, key []byte) (protocol.PartitionMetadata, error) {
	if len(key) == 0 {
		return protocol.PartitionMetadata{}, fmt.Errorf("automatic routing requires a non-empty message key")
	}
	partitions := make([]protocol.PartitionMetadata, 0)
	for _, partition := range metadata.Partitions {
		if partition.Topic == topicName {
			partitions = append(partitions, partition)
		}
	}
	if len(partitions) == 0 {
		return protocol.PartitionMetadata{}, fmt.Errorf("metadata contains no partitions for topic %q", topicName)
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i].PartitionID < partitions[j].PartitionID })
	hash := fnv.New32a()
	_, _ = hash.Write(key)
	index := int(hash.Sum32() % uint32(len(partitions)))
	return partitions[index], nil
}

// routeProduceAddress mirrors the broker's FNV-1a key partitioner and resolves
// the currently committed partition leader from a metadata snapshot.
func routeProduceAddress(bootstrapAddress, topicName string, key []byte) (string, uint32, error) {
	metadata, err := fetchMetadata(bootstrapAddress, topicName)
	if err != nil {
		return "", 0, err
	}
	selected, err := partitionForKey(metadata, topicName, key)
	if err != nil {
		return "", 0, err
	}
	for _, broker := range metadata.Brokers {
		if broker.ID == selected.Leader {
			return broker.Address, selected.PartitionID, nil
		}
	}
	return "", 0, fmt.Errorf("metadata has no address for partition leader %q", selected.Leader)
}

func runProduce(args []string) error {
	var address, topicName, key, value, ackText string
	var autoRoute bool
	if err := parseFlags("produce", args, func(fs *flag.FlagSet) error {
		fs.StringVar(&address, "addr", "127.0.0.1:9092", "broker address")
		fs.StringVar(&topicName, "topic", "", "topic name")
		fs.StringVar(&key, "key", "", "optional message key")
		fs.StringVar(&value, "value", "", "message value")
		fs.StringVar(&ackText, "acks", "leader", "acknowledgement mode: none, leader, all")
		fs.BoolVar(&autoRoute, "auto-route", true, "route keyed messages using current cluster metadata")
		return nil
	}); err != nil {
		return err
	}
	if topicName == "" {
		return fmt.Errorf("-topic is required")
	}
	var ack protocol.AckMode
	switch ackText {
	case "none":
		ack = protocol.AckNone
	case "leader":
		ack = protocol.AckLeader
	case "all":
		ack = protocol.AckAll
	default:
		return fmt.Errorf("invalid -acks value %q", ackText)
	}
	var messageKey []byte
	if key != "" {
		messageKey = []byte(key)
	}
	payload, err := protocol.EncodeProduceRequest(protocol.ProduceRequest{AckMode: ack, Batch: protocol.MessageBatch{Messages: []protocol.Message{{Topic: topicName, Key: messageKey, Value: []byte(value), Timestamp: time.Now().UnixMilli()}}}})
	if err != nil {
		return err
	}
	targetAddress := address
	routedPartition := uint32(0)
	wasRouted := autoRoute && len(messageKey) > 0
	if wasRouted {
		targetAddress, routedPartition, err = routeProduceAddress(address, topicName, messageKey)
		if err != nil {
			return fmt.Errorf("resolve partition leader: %w", err)
		}
		fmt.Printf("routing topic=%s partition=%d leader_address=%s\n", topicName, routedPartition, targetAddress)
	}
	response, err := request(targetAddress, protocol.APIPRODUCE, 1, payload)
	if err != nil && autoRoute && len(messageKey) > 0 {
		var brokerErr *brokerResponseError
		refresh := errors.As(err, &brokerErr) && (brokerErr.Code == protocol.ErrorNotPartitionLeader || brokerErr.Code == protocol.ErrorControllerAuthorityUnavailable)
		if refresh {
			var refreshedAddress string
			refreshedAddress, _, refreshErr := routeProduceAddress(address, topicName, messageKey)
			if refreshErr != nil {
				return errors.Join(err, fmt.Errorf("refresh metadata: %w", refreshErr))
			}
			response, err = request(refreshedAddress, protocol.APIPRODUCE, 1, payload)
		}
	}
	if err != nil {
		return err
	}
	result, err := protocol.DecodeProduceResponse(response.Payload)
	if err != nil {
		return err
	}
	for _, record := range result.Records {
		fmt.Printf("appended topic=%s partition=%d offset=%d\n", record.Topic, record.PartitionID, record.Offset)
	}
	return nil
}

func runConsume(args []string) error {
	var address, topicName string
	var partition uint
	var offset uint64
	var max uint
	if err := parseFlags("consume", args, func(fs *flag.FlagSet) error {
		fs.StringVar(&address, "addr", "127.0.0.1:9092", "broker address")
		fs.StringVar(&topicName, "topic", "", "topic name")
		fs.UintVar(&partition, "partition", 0, "partition ID")
		fs.Uint64Var(&offset, "offset", 0, "next offset to read")
		fs.UintVar(&max, "max", 10, "maximum messages")
		return nil
	}); err != nil {
		return err
	}
	if topicName == "" {
		return fmt.Errorf("-topic is required")
	}
	if max > uint(protocol.MaxConsumeMessages) {
		return fmt.Errorf("-max must be <= %d", protocol.MaxConsumeMessages)
	}
	payload, err := protocol.EncodeConsumeRequest(protocol.ConsumeRequest{Topic: topicName, PartitionID: uint32(partition), Offset: offset, MaxMessages: uint32(max)})
	if err != nil {
		return err
	}
	response, err := request(address, protocol.APICONSUME, 2, payload)
	if err != nil {
		return err
	}
	result, err := protocol.DecodeConsumeResponseV2(response.Payload)
	if err != nil {
		return err
	}
	for _, record := range result.Records {
		fmt.Printf("offset=%d timestamp=%d key=%s value=%s\n", record.Offset, record.Timestamp, strconv.Quote(string(record.Key)), strconv.Quote(string(record.Value)))
	}
	fmt.Printf("next_offset=%d log_end_offset=%d start_offset=%d\n", result.NextOffset, result.LogEndOffset, result.StartOffset)
	return nil
}

func runMetadata(args []string) error {
	var address, topicName string
	if err := parseFlags("metadata", args, func(fs *flag.FlagSet) error {
		fs.StringVar(&address, "addr", "127.0.0.1:9092", "broker address")
		fs.StringVar(&topicName, "topic", "", "optional topic filter")
		return nil
	}); err != nil {
		return err
	}
	payload, err := protocol.EncodeMetadataRequest(protocol.MetadataRequest{Topic: topicName})
	if err != nil {
		return err
	}
	response, err := request(address, protocol.APIMETADATA, protocol.MetadataAPIVersion, payload)
	if err != nil {
		return err
	}
	metadata, err := protocol.DecodeMetadataResponse(response.Payload)
	if err != nil {
		return err
	}
	fmt.Printf("metadata_version=%d\n", metadata.Version)
	for _, broker := range metadata.Brokers {
		fmt.Printf("broker id=%s address=%s\n", broker.ID, broker.Address)
	}
	for _, partition := range metadata.Partitions {
		fmt.Printf("topic=%s partition=%d leader=%s replicas=%v epoch=%d\n", partition.Topic, partition.PartitionID, partition.Leader, partition.Replicas, partition.LeaderEpoch)
	}
	return nil
}
