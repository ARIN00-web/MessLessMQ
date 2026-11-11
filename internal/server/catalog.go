package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/ARIN00-web/messlessmq/internal/topic"
)

const catalogVersion = 1

type TopicDefinition struct {
	Name       string `json:"name"`
	Partitions uint32 `json:"partitions"`
}

type topicCatalog struct {
	Version uint16            `json:"version"`
	Topics  []TopicDefinition `json:"topics"`
}

func ParseBootstrapTopics(value string) ([]TopicDefinition, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	parts := strings.Split(value, ",")
	definitions := make([]TopicDefinition, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		pieces := strings.Split(part, ":")
		if len(pieces) != 2 {
			return nil, fmt.Errorf("invalid bootstrap topic %q: expected name:partitions", part)
		}
		name := strings.TrimSpace(pieces[0])
		if err := topic.ValidateTopicName(name); err != nil {
			return nil, fmt.Errorf("invalid bootstrap topic %q: %w", name, err)
		}
		count, err := strconv.ParseUint(strings.TrimSpace(pieces[1]), 10, 32)
		if err != nil || count == 0 || count > 65536 {
			return nil, fmt.Errorf("invalid partition count for topic %q: expected 1-65536", name)
		}
		if _, exists := seen[name]; exists {
			return nil, fmt.Errorf("duplicate bootstrap topic %q", name)
		}
		seen[name] = struct{}{}
		definitions = append(definitions, TopicDefinition{Name: name, Partitions: uint32(count)})
	}
	return definitions, nil
}

func loadTopicCatalog(dataDir string, bootstrap []TopicDefinition) ([]TopicDefinition, error) {
	path := filepath.Join(dataDir, "metadata", "topics.json")
	data, err := os.ReadFile(path)
	if err == nil {
		var catalog topicCatalog
		if err := json.Unmarshal(data, &catalog); err != nil {
			return nil, fmt.Errorf("decode topic catalog: %w", err)
		}
		if catalog.Version != catalogVersion {
			return nil, fmt.Errorf("unsupported topic catalog version %d", catalog.Version)
		}
		definitions, err := validateTopicDefinitions(catalog.Topics)
		if err != nil {
			return nil, fmt.Errorf("validate persisted topic catalog: %w", err)
		}
		byName := make(map[string]TopicDefinition, len(definitions))
		for _, definition := range definitions {
			byName[definition.Name] = definition
		}
		changed := false
		for _, definition := range bootstrap {
			if _, err := validateTopicDefinitions([]TopicDefinition{definition}); err != nil {
				return nil, err
			}
			if existing, ok := byName[definition.Name]; ok {
				if existing.Partitions != definition.Partitions {
					return nil, fmt.Errorf("bootstrap topic %q requests %d partitions but persisted catalog has %d", definition.Name, definition.Partitions, existing.Partitions)
				}
				continue
			}
			byName[definition.Name] = definition
			definitions = append(definitions, definition)
			changed = true
		}
		if changed {
			if err := writeTopicCatalog(path, definitions); err != nil {
				return nil, err
			}
		}
		return definitions, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read topic catalog: %w", err)
	}
	definitions, err := validateTopicDefinitions(bootstrap)
	if err != nil {
		return nil, err
	}
	if err := writeTopicCatalog(path, definitions); err != nil {
		return nil, err
	}
	return definitions, nil
}

func validateTopicDefinitions(definitions []TopicDefinition) ([]TopicDefinition, error) {
	out := append([]TopicDefinition(nil), definitions...)
	seen := make(map[string]struct{}, len(out))
	for _, definition := range out {
		if err := topic.ValidateTopicName(definition.Name); err != nil {
			return nil, err
		}
		if definition.Partitions == 0 || definition.Partitions > 65536 {
			return nil, fmt.Errorf("topic %q partition count must be between 1 and 65536", definition.Name)
		}
		if _, exists := seen[definition.Name]; exists {
			return nil, fmt.Errorf("duplicate topic definition %q", definition.Name)
		}
		seen[definition.Name] = struct{}{}
	}
	return out, nil
}

func writeTopicCatalog(path string, definitions []TopicDefinition) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return fmt.Errorf("create topic metadata directory: %w", err)
	}
	data, err := json.MarshalIndent(topicCatalog{Version: catalogVersion, Topics: definitions}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode topic catalog: %w", err)
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".topics-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary topic catalog: %w", err)
	}
	tempPath := file.Name()
	cleanup := func() {
		_ = file.Close()
		_ = os.Remove(tempPath)
	}
	if err := file.Chmod(0600); err != nil {
		cleanup()
		return err
	}
	if _, err := file.Write(data); err != nil {
		cleanup()
		return fmt.Errorf("write topic catalog: %w", err)
	}
	if err := file.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("sync topic catalog: %w", err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("close topic catalog: %w", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("publish topic catalog: %w", err)
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync topic catalog directory: %w", err)
	}
	return nil
}
