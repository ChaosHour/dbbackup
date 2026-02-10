package backup

import (
	"testing"
)

func TestBLOBPackSerializeDeserialize(t *testing.T) {
	pack := NewBLOBPack(3)
	pack.Add(100, []byte("hello"))
	pack.Add(200, []byte("world"))
	pack.Add(300, []byte("!"))

	if pack.Count() != 3 {
		t.Fatalf("expected 3 entries, got %d", pack.Count())
	}
	if pack.DataSize() != 11 {
		t.Fatalf("expected data size 11, got %d", pack.DataSize())
	}
	if !pack.IsFull() {
		t.Fatal("expected pack to be full")
	}

	data, err := pack.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// Deserialize
	pack2, err := DeserializeBLOBPack(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	entries := pack2.Entries()
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	tests := []struct {
		oid  uint64
		data string
	}{
		{100, "hello"},
		{200, "world"},
		{300, "!"},
	}
	for i, tt := range tests {
		if entries[i].OID != tt.oid {
			t.Errorf("entry %d: expected OID %d, got %d", i, tt.oid, entries[i].OID)
		}
		if string(entries[i].Data) != tt.data {
			t.Errorf("entry %d: expected data %q, got %q", i, tt.data, string(entries[i].Data))
		}
	}
}

func TestBLOBPackEmpty(t *testing.T) {
	pack := NewBLOBPack(100)
	if pack.Count() != 0 {
		t.Fatalf("expected 0 entries, got %d", pack.Count())
	}
	if pack.IsFull() {
		t.Fatal("expected pack to not be full")
	}

	data, err := pack.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	pack2, err := DeserializeBLOBPack(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}
	if pack2.Count() != 0 {
		t.Fatalf("expected 0 entries after deserialize, got %d", pack2.Count())
	}
}

func TestBLOBPackLargeData(t *testing.T) {
	pack := NewBLOBPack(2)

	// 1MB blob
	big := make([]byte, 1024*1024)
	for i := range big {
		big[i] = byte(i % 256)
	}
	pack.Add(42, big)
	pack.Add(99, []byte{0xFF, 0x00, 0xAA})

	data, err := pack.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	pack2, err := DeserializeBLOBPack(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	entries := pack2.Entries()
	if len(entries[0].Data) != 1024*1024 {
		t.Fatalf("expected 1MB blob, got %d bytes", len(entries[0].Data))
	}
	// Verify content integrity
	for i, b := range entries[0].Data {
		if b != byte(i%256) {
			t.Fatalf("data corruption at byte %d: expected %d, got %d", i, byte(i%256), b)
		}
	}
	if entries[1].OID != 99 || len(entries[1].Data) != 3 {
		t.Fatalf("second entry incorrect: OID=%d, len=%d", entries[1].OID, len(entries[1].Data))
	}
}

func TestBLOBPackInvalidMagic(t *testing.T) {
	_, err := DeserializeBLOBPack([]byte("INVALIDX" + "\x01\x00\x00\x00" + "\x00\x00\x00\x00"))
	if err == nil {
		t.Fatal("expected error for invalid magic")
	}
}

func TestBLOBStrategyString(t *testing.T) {
	tests := []struct {
		s    BLOBStrategy
		want string
	}{
		{BLOBStrategyStandard, "standard"},
		{BLOBStrategyBundle, "bundle"},
		{BLOBStrategyParallelStream, "parallel-stream"},
		{BLOBStrategyLargeObject, "large-object"},
	}
	for _, tt := range tests {
		if got := tt.s.String(); got != tt.want {
			t.Errorf("BLOBStrategy(%d).String() = %q, want %q", tt.s, got, tt.want)
		}
	}
}
