package bond

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-bond/bond/utils"
)

const (
	bondInitializationIntentVersion = 1
	bondInitializationIntentPrefix  = ".bond-initialization-intent-v1-"
	bondInitializationIntentSuffix  = ".json"
	bondInitializationIntentIDBytes = 16
)

type bondInitializationIntentRecord struct {
	Version int    `json:"version"`
	ID      string `json:"id"`
}

type bondInitializationIntent struct{}

func inspectBondInitializationIntent(dirname string) (*bondInitializationIntent, error) {
	directory := filepath.Join(dirname, "bond")
	entries, err := os.ReadDir(directory)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("bond: inspect external initialization intent: %w", err)
	}
	var intent *bondInitializationIntent
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), bondInitializationIntentPrefix) {
			continue
		}
		entryInfo, err := entry.Info()
		if err != nil {
			return nil, fmt.Errorf("bond: inspect external initialization intent file: %w", err)
		}
		if !entryInfo.Mode().IsRegular() {
			return nil, errors.New("bond: external initialization intent is not a regular file")
		}
		if intent != nil {
			return nil, errors.New("bond: multiple external initialization intents are present")
		}
		path := filepath.Join(directory, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("bond: read external initialization intent: %w", err)
		}
		if err := validateBondInitializationIntent(entry.Name(), data); err != nil {
			return nil, err
		}
		intent = &bondInitializationIntent{}
	}
	return intent, nil
}

func createBondInitializationIntent(dirname string) (*bondInitializationIntent, error) {
	idBytes := make([]byte, bondInitializationIntentIDBytes)
	if _, err := io.ReadFull(rand.Reader, idBytes); err != nil {
		return nil, fmt.Errorf("bond: generate external initialization intent identity: %w", err)
	}
	id := hex.EncodeToString(idBytes)
	record := bondInitializationIntentRecord{Version: bondInitializationIntentVersion, ID: id}
	data, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("bond: marshal external initialization intent: %w", err)
	}
	data = append(data, '\n')
	path := filepath.Join(
		dirname,
		"bond",
		bondInitializationIntentPrefix+id+bondInitializationIntentSuffix,
	)
	if err := utils.WriteFileWithSync(path, data, 0o600); err != nil {
		return nil, fmt.Errorf("bond: publish external initialization intent: %w", err)
	}
	return &bondInitializationIntent{}, nil
}

func validateBondInitializationIntent(name string, data []byte) error {
	if !strings.HasSuffix(name, bondInitializationIntentSuffix) {
		return errors.New("bond: external initialization intent has an invalid filename")
	}
	id := strings.TrimSuffix(strings.TrimPrefix(name, bondInitializationIntentPrefix), bondInitializationIntentSuffix)
	decodedID, err := hex.DecodeString(id)
	if err != nil || len(decodedID) != bondInitializationIntentIDBytes {
		return errors.New("bond: external initialization intent has an invalid identity")
	}
	var record bondInitializationIntentRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return fmt.Errorf("bond: decode external initialization intent: %w", err)
	}
	if record.Version != bondInitializationIntentVersion {
		return fmt.Errorf("bond: unsupported external initialization intent version %d", record.Version)
	}
	if record.ID != id {
		return errors.New("bond: external initialization intent identity does not match its filename")
	}
	expected, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("bond: re-encode external initialization intent: %w", err)
	}
	expected = append(expected, '\n')
	if !bytes.Equal(data, expected) {
		return errors.New("bond: external initialization intent is not canonically encoded")
	}
	return nil
}
