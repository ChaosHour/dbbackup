package native

import (
	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

// ═══════════════════════════════════════════════════════════════════════════════
// CUSTOM FORMAT — TABLE OF CONTENTS (TOC)
//
// PostgreSQL-compatible custom archive format (.dump / -Fc).
//
// File layout:
//
//   ┌──────────────────────────────────────┐
//   │ Header (32 bytes)                    │  Magic + version + compression info
//   ├──────────────────────────────────────┤
//   │ Data Block 1 (variable)             │  Compressed COPY/SQL data
//   │ Data Block 2 (variable)             │
//   │  ...                                │
//   │ Data Block N (variable)             │
//   ├──────────────────────────────────────┤
//   │ TOC Directory (variable)            │  Serialized TOC entries
//   ├──────────────────────────────────────┤
//   │ Footer (24 bytes)                   │  TOC offset + count + checksum
//   └──────────────────────────────────────┘
//
// ═══════════════════════════════════════════════════════════════════════════════

// Archive magic bytes
var customFormatMagic = [5]byte{'P', 'G', 'D', 'M', 'P'}

// Format version — our own versioning (not tied to PostgreSQL's)
const customFormatVersion uint8 = 1

// Section constants matching PostgreSQL's dump sections
const (
	SectionPreData  uint8 = 1 // CREATE TABLE, TYPE, SEQUENCE, FUNCTION, SCHEMA
	SectionData     uint8 = 2 // COPY data
	SectionPostData uint8 = 3 // CREATE INDEX, ADD CONSTRAINT, TRIGGER
	SectionNone     uint8 = 0 // Metadata-only entries (SET, COMMENT)
)

// Compression method constants
const (
	CompressNone uint8 = 0
	CompressGzip uint8 = 1
	CompressZstd uint8 = 2
	CompressLZ4  uint8 = 3
)

// CustomFormatHeader is the 32-byte file header
type CustomFormatHeader struct {
	Magic       [5]byte // "PGDMP"
	Version     uint8   // Format version (1)
	Flags       uint8   // Bit 0: has TOC, Bit 1: has data
	Compression uint8   // CompressNone / CompressGzip / CompressZstd / CompressLZ4
	CompLevel   uint8   // Compression level (0-22)
	IntSize     uint8   // Size of integers in archive (8 = 64-bit)
	OffSize     uint8   // Size of offsets in archive (8 = 64-bit)
	DBName      [20]byte // Database name (truncated/padded)
	_           byte    // Padding to 32 bytes
}

// headerSize is the fixed size of the archive header
const headerSize = 32

// TOCEntry represents a single object in the archive's table of contents
type TOCEntry struct {
	DumpID       int32    // Unique sequential ID for this object
	Tag          string   // Object name (e.g., "users", "idx_users_email")
	Namespace    string   // Schema name (e.g., "public")
	Desc         string   // Object type description ("TABLE", "INDEX", "SEQUENCE", etc.)
	Owner        string   // Object owner
	Section      uint8    // SectionPreData, SectionData, SectionPostData
	Defn         string   // SQL definition (CREATE TABLE ..., CREATE INDEX ...)
	DropStmt     string   // DROP statement for clean restore
	CopyStmt     string   // COPY ... FROM stdin statement (only for data entries)
	Dependencies []int32  // DumpIDs this entry depends on (for parallel restore ordering)
	DataOffset   int64    // Byte offset to data block in archive (0 = no data)
	DataLength   int64    // Uncompressed data length in bytes
	DataCompLen  int64    // Compressed data length (equals DataLength if uncompressed)
	RowCount     int64    // Number of rows (only for data entries)
	TableName    string   // Full qualified table name for data entries
}

// TOCEntryType returns a human-readable type for logging
func (t *TOCEntry) TOCEntryType() string {
	switch t.Section {
	case SectionPreData:
		return "pre-data"
	case SectionData:
		return "data"
	case SectionPostData:
		return "post-data"
	default:
		return "other"
	}
}

// TOC is the complete table of contents for a custom format archive
type TOC struct {
	Entries  []*TOCEntry
	nextID   int32
	entryMap map[int32]*TOCEntry // DumpID → entry for fast lookup
}

// NewTOC creates an empty TOC
func NewTOC() *TOC {
	return &TOC{
		nextID:   1,
		entryMap: make(map[int32]*TOCEntry),
	}
}

// AddEntry adds a new entry and returns its assigned DumpID
func (toc *TOC) AddEntry(entry *TOCEntry) int32 {
	entry.DumpID = toc.nextID
	toc.nextID++
	toc.Entries = append(toc.Entries, entry)
	toc.entryMap[entry.DumpID] = entry
	return entry.DumpID
}

// GetEntry retrieves an entry by DumpID
func (toc *TOC) GetEntry(dumpID int32) *TOCEntry {
	return toc.entryMap[dumpID]
}

// CountBySection returns the number of entries in each section
func (toc *TOC) CountBySection() (preData, data, postData int) {
	for _, e := range toc.Entries {
		switch e.Section {
		case SectionPreData:
			preData++
		case SectionData:
			data++
		case SectionPostData:
			postData++
		}
	}
	return
}

// DataEntries returns only entries with data (Section == SectionData)
func (toc *TOC) DataEntries() []*TOCEntry {
	var result []*TOCEntry
	for _, e := range toc.Entries {
		if e.Section == SectionData && e.DataOffset > 0 {
			result = append(result, e)
		}
	}
	return result
}

// PreDataEntries returns pre-data entries in dependency order
func (toc *TOC) PreDataEntries() []*TOCEntry {
	var result []*TOCEntry
	for _, e := range toc.Entries {
		if e.Section == SectionPreData {
			result = append(result, e)
		}
	}
	return result
}

// PostDataEntries returns post-data entries in dependency order
func (toc *TOC) PostDataEntries() []*TOCEntry {
	var result []*TOCEntry
	for _, e := range toc.Entries {
		if e.Section == SectionPostData {
			result = append(result, e)
		}
	}
	return result
}

// FilterByTags returns entries matching specific object names
func (toc *TOC) FilterByTags(tags []string) []*TOCEntry {
	tagSet := make(map[string]bool, len(tags))
	for _, t := range tags {
		tagSet[t] = true
	}

	var result []*TOCEntry
	for _, e := range toc.Entries {
		if tagSet[e.Tag] || tagSet[e.Namespace+"."+e.Tag] {
			result = append(result, e)
		}
	}
	return result
}

// FilterBySection returns entries for a specific section
func (toc *TOC) FilterBySection(section uint8) []*TOCEntry {
	var result []*TOCEntry
	for _, e := range toc.Entries {
		if e.Section == section {
			result = append(result, e)
		}
	}
	return result
}

// TopologicalOrder returns data entries sorted by dependencies (safe restore order).
// Entries with no dependencies come first; entries that depend on others come later.
func (toc *TOC) TopologicalOrder() []*TOCEntry {
	entries := toc.DataEntries()
	if len(entries) == 0 {
		return entries
	}

	// Build adjacency: entryIdx depends on depIdx
	idxMap := make(map[int32]int, len(entries))
	for i, e := range entries {
		idxMap[e.DumpID] = i
	}

	inDegree := make([]int, len(entries))
	forward := make(map[int][]int) // depIdx → [dependents]

	for i, e := range entries {
		for _, depID := range e.Dependencies {
			depIdx, ok := idxMap[depID]
			if !ok {
				continue // dependency not in data set (pre-data, already handled)
			}
			forward[depIdx] = append(forward[depIdx], i)
			inDegree[i]++
		}
	}

	// Kahn's algorithm
	var queue []int
	for i, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, i)
		}
	}

	sorted := make([]*TOCEntry, 0, len(entries))
	for len(queue) > 0 {
		idx := queue[0]
		queue = queue[1:]
		sorted = append(sorted, entries[idx])

		for _, depIdx := range forward[idx] {
			inDegree[depIdx]--
			if inDegree[depIdx] == 0 {
				queue = append(queue, depIdx)
			}
		}
	}

	// If cycle detected, append remaining entries
	if len(sorted) < len(entries) {
		seen := make(map[int32]bool, len(sorted))
		for _, e := range sorted {
			seen[e.DumpID] = true
		}
		for _, e := range entries {
			if !seen[e.DumpID] {
				sorted = append(sorted, e)
			}
		}
	}

	return sorted
}

// SortBySize returns data entries sorted by DataLength descending (largest first).
// Useful for parallel restore: start biggest tables first to minimize tail latency.
func (toc *TOC) SortBySize() []*TOCEntry {
	entries := toc.DataEntries()
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].DataLength > entries[j].DataLength
	})
	return entries
}

// ═══════════════════════════════════════════════════════════════════════════════
// BINARY SERIALIZATION
//
// All multi-byte integers are little-endian.
// Strings are length-prefixed: [int32 length][...bytes]
// Dependency arrays: [int32 count][...int32 ids]
// ═══════════════════════════════════════════════════════════════════════════════

// WriteHeader serializes the archive header
func WriteHeader(w io.Writer, hdr *CustomFormatHeader) error {
	hdr.Magic = customFormatMagic
	hdr.Version = customFormatVersion
	hdr.IntSize = 8
	hdr.OffSize = 8
	return binary.Write(w, binary.LittleEndian, hdr)
}

// ReadHeader deserializes the archive header
func ReadHeader(r io.Reader) (*CustomFormatHeader, error) {
	var hdr CustomFormatHeader
	if err := binary.Read(r, binary.LittleEndian, &hdr); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	if hdr.Magic != customFormatMagic {
		return nil, fmt.Errorf("invalid archive: expected magic PGDMP, got %q", hdr.Magic)
	}
	if hdr.Version != customFormatVersion {
		return nil, fmt.Errorf("unsupported format version %d (expected %d)", hdr.Version, customFormatVersion)
	}
	return &hdr, nil
}

// WriteTOCEntry serializes a single TOC entry
func WriteTOCEntry(w io.Writer, entry *TOCEntry) error {
	// DumpID
	if err := writeInt32(w, entry.DumpID); err != nil {
		return err
	}
	// Section
	if err := writeUint8(w, entry.Section); err != nil {
		return err
	}
	// Strings
	for _, s := range []string{entry.Tag, entry.Namespace, entry.Desc, entry.Owner, entry.Defn, entry.DropStmt, entry.CopyStmt, entry.TableName} {
		if err := writeString(w, s); err != nil {
			return err
		}
	}
	// Data offsets
	if err := writeInt64(w, entry.DataOffset); err != nil {
		return err
	}
	if err := writeInt64(w, entry.DataLength); err != nil {
		return err
	}
	if err := writeInt64(w, entry.DataCompLen); err != nil {
		return err
	}
	if err := writeInt64(w, entry.RowCount); err != nil {
		return err
	}
	// Dependencies
	if err := writeInt32(w, int32(len(entry.Dependencies))); err != nil {
		return err
	}
	for _, dep := range entry.Dependencies {
		if err := writeInt32(w, dep); err != nil {
			return err
		}
	}
	return nil
}

// ReadTOCEntry deserializes a single TOC entry
func ReadTOCEntry(r io.Reader) (*TOCEntry, error) {
	entry := &TOCEntry{}

	var err error
	// DumpID
	entry.DumpID, err = readInt32(r)
	if err != nil {
		return nil, fmt.Errorf("read DumpID: %w", err)
	}
	// Section
	entry.Section, err = readUint8(r)
	if err != nil {
		return nil, fmt.Errorf("read Section: %w", err)
	}
	// Strings
	strs := []*string{&entry.Tag, &entry.Namespace, &entry.Desc, &entry.Owner, &entry.Defn, &entry.DropStmt, &entry.CopyStmt, &entry.TableName}
	for i, sp := range strs {
		*sp, err = readString(r)
		if err != nil {
			return nil, fmt.Errorf("read string field %d: %w", i, err)
		}
	}
	// Data offsets
	entry.DataOffset, err = readInt64(r)
	if err != nil {
		return nil, fmt.Errorf("read DataOffset: %w", err)
	}
	entry.DataLength, err = readInt64(r)
	if err != nil {
		return nil, fmt.Errorf("read DataLength: %w", err)
	}
	entry.DataCompLen, err = readInt64(r)
	if err != nil {
		return nil, fmt.Errorf("read DataCompLen: %w", err)
	}
	entry.RowCount, err = readInt64(r)
	if err != nil {
		return nil, fmt.Errorf("read RowCount: %w", err)
	}
	// Dependencies
	depCount, err := readInt32(r)
	if err != nil {
		return nil, fmt.Errorf("read dep count: %w", err)
	}
	if depCount > 0 {
		entry.Dependencies = make([]int32, depCount)
		for i := int32(0); i < depCount; i++ {
			entry.Dependencies[i], err = readInt32(r)
			if err != nil {
				return nil, fmt.Errorf("read dep %d: %w", i, err)
			}
		}
	}
	return entry, nil
}

// Footer is appended after the TOC directory and allows seeking directly to it
type Footer struct {
	TOCOffset  int64  // Byte offset where TOC directory starts
	TOCCount   int32  // Number of TOC entries
	Checksum   uint32 // CRC32 of all TOC data (0 if not computed)
	FooterSize int32  // Size of footer (for backward compat)
}

const footerSize = 20 // 8 (TOCOffset) + 4 (TOCCount) + 4 (Checksum) + 4 (FooterSize)

// WriteFooter serializes the archive footer
func WriteFooter(w io.Writer, footer *Footer) error {
	footer.FooterSize = int32(footerSize)
	if err := writeInt64(w, footer.TOCOffset); err != nil {
		return err
	}
	if err := writeInt32(w, footer.TOCCount); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, footer.Checksum); err != nil {
		return err
	}
	return writeInt32(w, footer.FooterSize)
}

// ReadFooter reads the footer from the end of the archive.
// The reader must support io.ReadSeeker to find the footer.
func ReadFooter(rs io.ReadSeeker) (*Footer, error) {
	// Seek to footerSize bytes before end
	if _, err := rs.Seek(-int64(footerSize), io.SeekEnd); err != nil {
		return nil, fmt.Errorf("seek to footer: %w", err)
	}

	footer := &Footer{}
	var err error
	footer.TOCOffset, err = readInt64(rs)
	if err != nil {
		return nil, fmt.Errorf("read TOCOffset: %w", err)
	}
	footer.TOCCount, err = readInt32(rs)
	if err != nil {
		return nil, fmt.Errorf("read TOCCount: %w", err)
	}
	if err := binary.Read(rs, binary.LittleEndian, &footer.Checksum); err != nil {
		return nil, fmt.Errorf("read Checksum: %w", err)
	}
	footer.FooterSize, err = readInt32(rs)
	if err != nil {
		return nil, fmt.Errorf("read FooterSize: %w", err)
	}

	return footer, nil
}

// ═══════════════════════════════════════════════════════════════════════════════
// BINARY PRIMITIVES
// ═══════════════════════════════════════════════════════════════════════════════

func writeUint8(w io.Writer, v uint8) error {
	_, err := w.Write([]byte{v})
	return err
}

func readUint8(r io.Reader) (uint8, error) {
	buf := [1]byte{}
	_, err := io.ReadFull(r, buf[:])
	return buf[0], err
}

func writeInt32(w io.Writer, v int32) error {
	return binary.Write(w, binary.LittleEndian, v)
}

func readInt32(r io.Reader) (int32, error) {
	var v int32
	err := binary.Read(r, binary.LittleEndian, &v)
	return v, err
}

func writeInt64(w io.Writer, v int64) error {
	return binary.Write(w, binary.LittleEndian, v)
}

func readInt64(r io.Reader) (int64, error) {
	var v int64
	err := binary.Read(r, binary.LittleEndian, &v)
	return v, err
}

func writeString(w io.Writer, s string) error {
	if err := writeInt32(w, int32(len(s))); err != nil {
		return err
	}
	if len(s) > 0 {
		_, err := w.Write([]byte(s))
		return err
	}
	return nil
}

func readString(r io.Reader) (string, error) {
	length, err := readInt32(r)
	if err != nil {
		return "", err
	}
	if length == 0 {
		return "", nil
	}
	if length < 0 || length > 100*1024*1024 { // 100MB safety limit
		return "", fmt.Errorf("invalid string length: %d", length)
	}
	buf := make([]byte, length)
	_, err = io.ReadFull(r, buf)
	return string(buf), err
}
