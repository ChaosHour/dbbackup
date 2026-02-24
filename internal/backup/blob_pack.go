// Package backup — BLOB Pack format for bundling small BLOBs into compressed packs
//
// Pack format:
//
//	[Magic: 8 bytes "BLOBPACK"]
//	[Version: 4 bytes uint32]
//	[Count: 4 bytes uint32]
//	[Index: count × 24 bytes (oid uint64 + offset uint64 + size uint64)]
//	[Data: concatenated BLOBs]
package backup

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

var blobPackMagic = [8]byte{'B', 'L', 'O', 'B', 'P', 'A', 'C', 'K'}

const blobPackVersion = 1

// BLOBPackEntry represents a single BLOB in a pack
type BLOBPackEntry struct {
	OID  uint64
	Data []byte
}

// BLOBPack bundles multiple small BLOBs into a single compressed unit
type BLOBPack struct {
	maxEntries int
	entries    []BLOBPackEntry
	dataSize   int64
}

// NewBLOBPack creates a new BLOB pack with the given capacity
func NewBLOBPack(maxEntries int) *BLOBPack {
	return &BLOBPack{
		maxEntries: maxEntries,
		entries:    make([]BLOBPackEntry, 0, maxEntries),
	}
}

// Add appends a BLOB to the pack. Returns true if the pack is now full.
func (p *BLOBPack) Add(oid uint64, data []byte) bool {
	p.entries = append(p.entries, BLOBPackEntry{OID: oid, Data: data})
	p.dataSize += int64(len(data))
	return len(p.entries) >= p.maxEntries
}

// IsFull returns true if the pack has reached capacity
func (p *BLOBPack) IsFull() bool {
	return len(p.entries) >= p.maxEntries
}

// Count returns the number of entries in the pack
func (p *BLOBPack) Count() int {
	return len(p.entries)
}

// DataSize returns the total uncompressed data size
func (p *BLOBPack) DataSize() int64 {
	return p.dataSize
}

// Reset clears the pack for reuse
func (p *BLOBPack) Reset() {
	p.entries = p.entries[:0]
	p.dataSize = 0
}

// Serialize writes the pack to a binary format suitable for compression
func (p *BLOBPack) Serialize() ([]byte, error) {
	// Calculate total size
	headerSize := 8 + 4 + 4                // magic + version + count
	indexSize := len(p.entries) * 24        // oid(8) + offset(8) + size(8)
	totalSize := headerSize + indexSize + int(p.dataSize)

	buf := make([]byte, 0, totalSize)
	w := bytes.NewBuffer(buf)

	// Write header
	w.Write(blobPackMagic[:])
	_ = binary.Write(w, binary.LittleEndian, uint32(blobPackVersion))
	_ = binary.Write(w, binary.LittleEndian, uint32(len(p.entries)))

	// Write index (pre-calculate offsets)
	offset := uint64(0)
	for _, entry := range p.entries {
		_ = binary.Write(w, binary.LittleEndian, entry.OID)
		_ = binary.Write(w, binary.LittleEndian, offset)
		_ = binary.Write(w, binary.LittleEndian, uint64(len(entry.Data)))
		offset += uint64(len(entry.Data))
	}

	// Write data
	for _, entry := range p.entries {
		w.Write(entry.Data)
	}

	return w.Bytes(), nil
}

// DeserializeBLOBPack reads a BLOB pack from binary data
func DeserializeBLOBPack(data []byte) (*BLOBPack, error) {
	r := bytes.NewReader(data)

	// Read and verify magic
	var magic [8]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return nil, fmt.Errorf("failed to read magic: %w", err)
	}
	if magic != blobPackMagic {
		return nil, fmt.Errorf("invalid BLOB pack magic: %v", magic)
	}

	// Read version
	var version uint32
	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		return nil, fmt.Errorf("failed to read version: %w", err)
	}
	if version != blobPackVersion {
		return nil, fmt.Errorf("unsupported BLOB pack version: %d", version)
	}

	// Read count
	var count uint32
	if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
		return nil, fmt.Errorf("failed to read entry count: %w", err)
	}

	// Read index
	type indexEntry struct {
		OID    uint64
		Offset uint64
		Size   uint64
	}
	index := make([]indexEntry, count)
	for i := uint32(0); i < count; i++ {
		if err := binary.Read(r, binary.LittleEndian, &index[i].OID); err != nil {
			return nil, fmt.Errorf("failed to read index entry %d: %w", i, err)
		}
		if err := binary.Read(r, binary.LittleEndian, &index[i].Offset); err != nil {
			return nil, fmt.Errorf("failed to read index entry %d: %w", i, err)
		}
		if err := binary.Read(r, binary.LittleEndian, &index[i].Size); err != nil {
			return nil, fmt.Errorf("failed to read index entry %d: %w", i, err)
		}
	}

	// Read data section
	dataStart := int64(8 + 4 + 4 + int(count)*24) // After header + index
	pack := &BLOBPack{
		maxEntries: int(count),
		entries:    make([]BLOBPackEntry, count),
	}

	for i, idx := range index {
		blob := make([]byte, idx.Size)
		readPos := dataStart + int64(idx.Offset)
		if _, err := r.Seek(readPos, io.SeekStart); err != nil {
			return nil, fmt.Errorf("failed to seek to BLOB %d: %w", i, err)
		}
		if _, err := io.ReadFull(r, blob); err != nil {
			return nil, fmt.Errorf("failed to read BLOB %d data: %w", i, err)
		}
		pack.entries[i] = BLOBPackEntry{OID: idx.OID, Data: blob}
		pack.dataSize += int64(idx.Size)
	}

	return pack, nil
}

// Entries returns the entries in the pack (for iteration during restore)
func (p *BLOBPack) Entries() []BLOBPackEntry {
	return p.entries
}
