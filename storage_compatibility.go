package bond

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/colblk"
	"github.com/cockroachdb/pebble/vfs"
)

const (
	// StorageReaderEpoch identifies the production set of storage readers. The
	// first explicit epoch contains only Pebble's legacy/default key schema.
	StorageReaderEpoch uint32 = 1

	// StorageCompatibilityFile is stored below the Bond metadata directory in
	// databases and checkpoints.
	StorageCompatibilityFile = "STORAGE_COMPATIBILITY.json"

	defaultKeySchemaBundleSize = 16
)

// StorageCompatibility describes the readers needed to open a checkpoint or
// backup. A zero ReaderEpoch is the backward-compatible legacy metadata form.
type StorageCompatibility struct {
	ReaderEpoch       uint32   `json:"reader_epoch"`
	FormatMajor       uint64   `json:"format_major"`
	RequiredKeySchema []string `json:"required_key_schemas"`
}

// StorageSchemaUsage aggregates bounded SST diagnostics by durable schema
// name. Live diagnostics use Pebble's logical table size; offline diagnostics
// use the physical size of each inspected SST.
type StorageSchemaUsage struct {
	Name  string `json:"name"`
	Files int    `json:"files"`
	Bytes uint64 `json:"bytes"`
}

// StorageSchemaRoute is reserved for a future stock-Pebble routing phase.
// Phase 5 catalog family assignments are descriptive and still report an
// empty route list explicitly.
type StorageSchemaRoute struct {
	Start  []byte `json:"start"`
	End    []byte `json:"end"`
	Schema string `json:"schema"`
}

// StorageDiagnostics reports the process configuration and the schemas found
// in current SST properties. It is intended for inspection output, not metric
// labels whose cardinality would be controlled by stored schema names.
type StorageDiagnostics struct {
	ReaderEpoch           uint32               `json:"reader_epoch"`
	FormatMajor           uint64               `json:"format_major"`
	ActiveWriter          string               `json:"active_writer"`
	RegisteredReaders     []string             `json:"registered_readers"`
	EncounteredSSTSchemas []StorageSchemaUsage `json:"encountered_sst_schemas"`
	UnknownSchemas        []string             `json:"unknown_schemas"`
	CatalogRoutes         []StorageSchemaRoute `json:"catalog_routes"`
}

type productionSchemaRegistry struct {
	epoch      uint32
	active     string
	readers    sstable.KeySchemas
	readerName []string
}

// DefaultKeySchemaName returns the durable name reserved for Bond's current
// production writer and reader.
func DefaultKeySchemaName() string {
	legacy := colblk.DefaultKeySchema(DefaultKeyComparer(), defaultKeySchemaBundleSize)
	return legacy.Name
}

func newProductionSchemaRegistry(comparer *pebble.Comparer) productionSchemaRegistry {
	legacy := colblk.DefaultKeySchema(comparer, defaultKeySchemaBundleSize)
	return productionSchemaRegistry{
		epoch:      StorageReaderEpoch,
		active:     legacy.Name,
		readers:    sstable.MakeKeySchemas(&legacy),
		readerName: []string{legacy.Name},
	}
}

func (r productionSchemaRegistry) install(opts *pebble.Options) error {
	if opts.KeySchema != "" {
		if opts.KeySchema == r.active {
			return fmt.Errorf(
				"bond: reserved production key schema %q may not be caller-supplied; Bond installs a fresh private definition during preparation",
				r.active,
			)
		}
		return fmt.Errorf("bond: production writer schema %q is unsupported; active writer remains %q", opts.KeySchema, r.active)
	}
	for name := range opts.KeySchemas {
		if name == r.active {
			return fmt.Errorf(
				"bond: reserved production key schema %q may not be caller-supplied; Bond installs a fresh private definition during preparation",
				r.active,
			)
		}
		return fmt.Errorf("bond: production reader schema %q is unsupported; registered readers remain %v", name, r.readerName)
	}
	opts.KeySchema = r.active
	opts.KeySchemas = r.readers
	return nil
}

func productionPebbleOptions(requested *pebble.Options, readOnly bool) (*pebble.Options, productionSchemaRegistry, error) {
	var opts *pebble.Options
	if requested == nil {
		opts = BuildPebbleOptions(MediumPerformance)
	} else {
		opts = requested.Clone()
	}

	opts.Comparer = DefaultKeyComparer()
	opts.FormatMajorVersion = PebbleDBFormat
	opts.SpanPolicyFunc = spanPolicyFunc
	opts.ReadOnly = readOnly
	registry := newProductionSchemaRegistry(opts.Comparer)
	if err := registry.install(opts); err != nil {
		return nil, productionSchemaRegistry{}, err
	}
	opts.EnsureDefaults()
	if err := opts.Validate(); err != nil {
		return nil, productionSchemaRegistry{}, fmt.Errorf("bond: validate production Pebble options: %w", err)
	}
	return opts, registry, nil
}

func inspectPebbleStorage(
	db *pebble.DB, format pebble.FormatMajorVersion, active string, registered []string,
) (StorageDiagnostics, error) {
	levels, err := db.SSTables(pebble.WithProperties())
	if err != nil {
		return StorageDiagnostics{}, fmt.Errorf("bond: inspect Pebble SSTs: %w", err)
	}

	usageByName := make(map[string]StorageSchemaUsage)
	for _, level := range levels {
		for _, table := range level {
			if table.Properties == nil {
				return StorageDiagnostics{}, fmt.Errorf("bond: SST %s has no table properties", table.FileNum)
			}
			name := table.Properties.KeySchemaName
			if name == "" {
				// Pre-columnar tables have no property but are decoded by the
				// comparer-derived default schema.
				name = active
			}
			usage := usageByName[name]
			usage.Name = name
			usage.Files++
			usage.Bytes += table.Size
			usageByName[name] = usage
		}
	}
	return storageDiagnosticsFromUsage(format, active, registered, usageByName), nil
}

func storageDiagnosticsFromUsage(
	format pebble.FormatMajorVersion,
	active string,
	registered []string,
	usageByName map[string]StorageSchemaUsage,
) StorageDiagnostics {
	registered = append([]string(nil), registered...)
	sort.Strings(registered)
	registeredSet := make(map[string]struct{}, len(registered))
	for _, name := range registered {
		registeredSet[name] = struct{}{}
	}
	unknownSet := make(map[string]struct{})
	for name := range usageByName {
		if _, ok := registeredSet[name]; !ok {
			unknownSet[name] = struct{}{}
		}
	}

	encountered := make([]StorageSchemaUsage, 0, len(usageByName))
	for _, usage := range usageByName {
		encountered = append(encountered, usage)
	}
	slices.SortFunc(encountered, func(a, b StorageSchemaUsage) int {
		if a.Name < b.Name {
			return -1
		}
		if a.Name > b.Name {
			return 1
		}
		return 0
	})
	unknown := make([]string, 0, len(unknownSet))
	for name := range unknownSet {
		unknown = append(unknown, name)
	}
	sort.Strings(unknown)

	return StorageDiagnostics{
		ReaderEpoch:           StorageReaderEpoch,
		FormatMajor:           uint64(format),
		ActiveWriter:          active,
		RegisteredReaders:     registered,
		EncounteredSSTSchemas: encountered,
		UnknownSchemas:        unknown,
		CatalogRoutes:         []StorageSchemaRoute{},
	}
}

// inspectPebbleStorageFiles reads only SST metadata blocks. Unknown columnar
// schemas are represented by name-only placeholders, so their key seekers are
// never initialized or executed during offline inspection.
func inspectPebbleStorageFiles(
	dirname string, format pebble.FormatMajorVersion, active string, registered []string,
) (StorageDiagnostics, error) {
	opts, _, err := productionPebbleOptions(LowPerformancePebbleOptions(), true)
	if err != nil {
		return StorageDiagnostics{}, err
	}
	readerOpts := opts.MakeReaderOptions()
	usageByName := make(map[string]StorageSchemaUsage)
	err = filepath.WalkDir(dirname, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.EqualFold(filepath.Ext(path), ".sst") {
			return nil
		}
		properties, size, err := readSSTProperties(path, readerOpts)
		if err != nil {
			return fmt.Errorf("bond: inspect SST %s: %w", path, err)
		}
		name := properties.KeySchemaName
		if name == "" {
			name = active
		}
		usage := usageByName[name]
		usage.Name = name
		usage.Files++
		usage.Bytes += size
		usageByName[name] = usage
		return nil
	})
	if err != nil {
		return StorageDiagnostics{}, err
	}
	return storageDiagnosticsFromUsage(format, active, registered, usageByName), nil
}

func readSSTProperties(path string, baseOptions sstable.ReaderOptions) (*sstable.Properties, uint64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, 0, err
	}
	reader, unknownSchema, err := openSSTPropertiesReader(path, baseOptions)
	if err != nil {
		return nil, 0, err
	}
	if unknownSchema != "" {
		placeholder := &colblk.KeySchema{Name: unknownSchema}
		baseOptions.KeySchemas = cloneKeySchemas(baseOptions.KeySchemas)
		baseOptions.KeySchemas[unknownSchema] = placeholder
		reader, unknownSchema, err = openSSTPropertiesReader(path, baseOptions)
		if err != nil {
			return nil, 0, err
		}
		if unknownSchema != "" {
			return nil, 0, fmt.Errorf("could not register name-only schema %q", unknownSchema)
		}
	}
	properties, err := reader.ReadPropertiesBlock(context.Background(), nil)
	closeErr := reader.Close()
	if err != nil {
		return nil, 0, err
	}
	if closeErr != nil {
		return nil, 0, closeErr
	}
	return &properties, uint64(info.Size()), nil
}

func cloneKeySchemas(source sstable.KeySchemas) sstable.KeySchemas {
	cloned := make(sstable.KeySchemas, len(source)+1)
	for name, schema := range source {
		cloned[name] = schema
	}
	return cloned
}

func openSSTPropertiesReader(
	path string, options sstable.ReaderOptions,
) (reader *sstable.Reader, unknownSchema string, err error) {
	file, err := vfs.Default.Open(path)
	if err != nil {
		return nil, "", err
	}
	readable, err := objstorageprovider.NewFileReadable(
		file, vfs.Default, objstorageprovider.NewReadaheadConfig(), path,
	)
	if err != nil {
		_ = file.Close()
		return nil, "", err
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			_ = readable.Close()
			unknownSchema, err = unknownKeySchemaFromPanic(recovered)
			reader = nil
		}
	}()
	reader, err = sstable.NewReader(context.Background(), readable, options)
	if err != nil {
		_ = readable.Close()
	}
	return reader, "", err
}

func unknownKeySchemaFromPanic(recovered any) (string, error) {
	message := fmt.Sprint(recovered)
	const marker = "unknown key schema "
	start := strings.Index(message, marker)
	if start < 0 {
		return "", fmt.Errorf("unexpected panic while reading SST properties: %v", recovered)
	}
	quoted := message[start+len(marker):]
	if len(quoted) == 0 || quoted[0] != '"' {
		return "", fmt.Errorf("unknown key schema panic has no Go-quoted name: %q", message)
	}
	escaped := false
	for index := 1; index < len(quoted); index++ {
		switch {
		case escaped:
			escaped = false
		case quoted[index] == '\\':
			escaped = true
		case quoted[index] == '"':
			name, err := strconv.Unquote(quoted[:index+1])
			if err != nil || name == "" {
				return "", fmt.Errorf("parse unknown key schema panic %q: %w", message, err)
			}
			return name, nil
		}
	}
	return "", fmt.Errorf("unknown key schema panic has an unterminated name: %q", message)
}

func compatibilityFromDiagnostics(d StorageDiagnostics) StorageCompatibility {
	requiredSet := map[string]struct{}{d.ActiveWriter: {}}
	for _, name := range d.RegisteredReaders {
		requiredSet[name] = struct{}{}
	}
	for _, usage := range d.EncounteredSSTSchemas {
		requiredSet[usage.Name] = struct{}{}
	}
	required := make([]string, 0, len(requiredSet))
	for name := range requiredSet {
		required = append(required, name)
	}
	sort.Strings(required)
	return StorageCompatibility{
		ReaderEpoch:       d.ReaderEpoch,
		FormatMajor:       d.FormatMajor,
		RequiredKeySchema: required,
	}
}

// ValidateStorageCompatibility verifies that the production binary can read a
// backup/checkpoint before a restore mutates its destination.
func ValidateStorageCompatibility(compatibility StorageCompatibility) error {
	registry := newProductionSchemaRegistry(DefaultKeyComparer())
	if compatibility.ReaderEpoch > registry.epoch {
		return fmt.Errorf("bond: storage reader epoch %d is newer than supported epoch %d", compatibility.ReaderEpoch, registry.epoch)
	}
	if compatibility.FormatMajor < uint64(pebble.FormatMinSupported) {
		return fmt.Errorf(
			"bond: Pebble format %d is older than minimum supported format %d",
			compatibility.FormatMajor,
			pebble.FormatMinSupported,
		)
	}
	if compatibility.FormatMajor > uint64(PebbleDBFormat) {
		return fmt.Errorf("bond: Pebble format %d is newer than supported format %d", compatibility.FormatMajor, PebbleDBFormat)
	}

	required := compatibility.RequiredKeySchema
	if len(required) == 0 {
		return fmt.Errorf("bond: storage compatibility is missing required key schemas")
	}
	for index, name := range required {
		if name == "" {
			return fmt.Errorf("bond: storage compatibility contains an empty key schema name")
		}
		if index > 0 && name <= required[index-1] {
			return fmt.Errorf("bond: required key schemas must be strictly sorted and unique")
		}
	}
	missing := make([]string, 0)
	for _, name := range required {
		if _, ok := registry.readers[name]; !ok {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	missing = slices.Compact(missing)
	if len(missing) > 0 {
		return fmt.Errorf("bond: storage requires unregistered key schemas %v; available readers are %v", missing, registry.readerName)
	}
	return nil
}

// WriteStorageCompatibility writes deterministic checkpoint/database metadata.
func WriteStorageCompatibility(databaseDir string, compatibility StorageCompatibility) error {
	compatibility.RequiredKeySchema = append([]string(nil), compatibility.RequiredKeySchema...)
	sort.Strings(compatibility.RequiredKeySchema)
	compatibility.RequiredKeySchema = slices.Compact(compatibility.RequiredKeySchema)
	data, err := json.MarshalIndent(compatibility, "", "  ")
	if err != nil {
		return fmt.Errorf("bond: marshal storage compatibility: %w", err)
	}
	data = append(data, '\n')
	dir := filepath.Join(databaseDir, "bond")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("bond: create storage compatibility directory: %w", err)
	}
	if err := writeFileAtomically(
		filepath.Join(dir, StorageCompatibilityFile), data, 0o644, atomicFileWriteHooks{},
	); err != nil {
		return fmt.Errorf("bond: write storage compatibility: %w", err)
	}
	return nil
}

type atomicFileWriteHooks struct {
	beforeRename  func(string) error
	syncDirectory func(string) error
}

func writeFileAtomically(
	path string, data []byte, mode os.FileMode, hooks atomicFileWriteHooks,
) error {
	dir := filepath.Dir(path)
	existing, err := os.ReadFile(path)
	if err == nil && slices.Equal(existing, data) {
		return syncAtomicFileDirectory(dir, hooks)
	}
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	temporary, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	renamed := false
	defer func() {
		if !renamed {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(mode); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if hooks.beforeRename != nil {
		if err := hooks.beforeRename(temporaryPath); err != nil {
			return err
		}
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	renamed = true
	return syncAtomicFileDirectory(dir, hooks)
}

func syncAtomicFileDirectory(dir string, hooks atomicFileWriteHooks) error {
	if hooks.syncDirectory != nil {
		return hooks.syncDirectory(dir)
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// ReadStorageCompatibility reads optional checkpoint/database metadata. A nil
// result denotes a pre-Phase 4 legacy checkpoint.
func ReadStorageCompatibility(databaseDir string) (*StorageCompatibility, error) {
	data, err := os.ReadFile(filepath.Join(databaseDir, "bond", StorageCompatibilityFile))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("bond: read storage compatibility: %w", err)
	}
	var compatibility StorageCompatibility
	if err := json.Unmarshal(data, &compatibility); err != nil {
		return nil, fmt.Errorf("bond: decode storage compatibility: %w", err)
	}
	return &compatibility, nil
}
