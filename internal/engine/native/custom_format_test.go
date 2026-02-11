package native

import (
	"bytes"
	"io"
	"testing"
)

// ═══════════════════════════════════════════════════════════════════════════════
// CUSTOM FORMAT TESTS
//
// Tests the binary format: header, footer, TOC serialization/deserialization,
// and the full write+read round trip using in-memory archives.
// ═══════════════════════════════════════════════════════════════════════════════

func TestCustomFormatHeaderRoundTrip(t *testing.T) {
	var buf bytes.Buffer

	hdr := &CustomFormatHeader{
		Compression: CompressGzip,
		CompLevel:   6,
		Flags:       0x03,
	}
	copy(hdr.DBName[:], "testdb")

	if err := WriteHeader(&buf, hdr); err != nil {
		t.Fatalf("WriteHeader: %v", err)
	}

	if buf.Len() != headerSize {
		t.Fatalf("header size = %d, want %d", buf.Len(), headerSize)
	}

	// Read back
	got, err := ReadHeader(&buf)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}

	if got.Magic != customFormatMagic {
		t.Errorf("magic = %q, want %q", got.Magic, customFormatMagic)
	}
	if got.Version != customFormatVersion {
		t.Errorf("version = %d, want %d", got.Version, customFormatVersion)
	}
	if got.Compression != CompressGzip {
		t.Errorf("compression = %d, want %d", got.Compression, CompressGzip)
	}
	if got.CompLevel != 6 {
		t.Errorf("comp_level = %d, want 6", got.CompLevel)
	}
	if got.Flags != 0x03 {
		t.Errorf("flags = 0x%02x, want 0x03", got.Flags)
	}
	dbName := string(bytes.TrimRight(got.DBName[:], "\x00"))
	if dbName != "testdb" {
		t.Errorf("dbname = %q, want %q", dbName, "testdb")
	}
}

func TestCustomFormatInvalidMagic(t *testing.T) {
	data := make([]byte, headerSize)
	copy(data, "WRONG")

	_, err := ReadHeader(bytes.NewReader(data))
	if err == nil {
		t.Fatal("expected error for invalid magic")
	}
}

func TestTOCEntryRoundTrip(t *testing.T) {
	entry := &TOCEntry{
		DumpID:       42,
		Tag:          "users",
		Namespace:    "public",
		Desc:         "TABLE",
		Owner:        "postgres",
		Section:      SectionPreData,
		Defn:         "CREATE TABLE public.users (id serial PRIMARY KEY, name text);",
		DropStmt:     "DROP TABLE IF EXISTS public.users CASCADE;",
		CopyStmt:     "",
		TableName:    "public.users",
		Dependencies: []int32{1, 5, 10},
		DataOffset:   0,
		DataLength:   0,
		DataCompLen:  0,
		RowCount:     0,
	}

	var buf bytes.Buffer
	if err := WriteTOCEntry(&buf, entry); err != nil {
		t.Fatalf("WriteTOCEntry: %v", err)
	}

	got, err := ReadTOCEntry(&buf)
	if err != nil {
		t.Fatalf("ReadTOCEntry: %v", err)
	}

	if got.DumpID != 42 {
		t.Errorf("DumpID = %d, want 42", got.DumpID)
	}
	if got.Tag != "users" {
		t.Errorf("Tag = %q, want %q", got.Tag, "users")
	}
	if got.Namespace != "public" {
		t.Errorf("Namespace = %q, want %q", got.Namespace, "public")
	}
	if got.Desc != "TABLE" {
		t.Errorf("Desc = %q, want %q", got.Desc, "TABLE")
	}
	if got.Owner != "postgres" {
		t.Errorf("Owner = %q, want %q", got.Owner, "postgres")
	}
	if got.Section != SectionPreData {
		t.Errorf("Section = %d, want %d", got.Section, SectionPreData)
	}
	if got.Defn != entry.Defn {
		t.Errorf("Defn mismatch")
	}
	if got.DropStmt != entry.DropStmt {
		t.Errorf("DropStmt mismatch")
	}
	if got.TableName != "public.users" {
		t.Errorf("TableName = %q, want %q", got.TableName, "public.users")
	}
	if len(got.Dependencies) != 3 {
		t.Fatalf("Dependencies len = %d, want 3", len(got.Dependencies))
	}
	if got.Dependencies[0] != 1 || got.Dependencies[1] != 5 || got.Dependencies[2] != 10 {
		t.Errorf("Dependencies = %v, want [1 5 10]", got.Dependencies)
	}
}

func TestTOCEntryDataFields(t *testing.T) {
	entry := &TOCEntry{
		DumpID:      7,
		Tag:         "orders",
		Namespace:   "shop",
		Desc:        "TABLE DATA",
		Section:     SectionData,
		CopyStmt:    `COPY "shop"."orders" FROM stdin;`,
		TableName:   "shop.orders",
		DataOffset:  1024,
		DataLength:  500000,
		DataCompLen: 125000,
		RowCount:    10000,
	}

	var buf bytes.Buffer
	if err := WriteTOCEntry(&buf, entry); err != nil {
		t.Fatalf("WriteTOCEntry: %v", err)
	}

	got, err := ReadTOCEntry(&buf)
	if err != nil {
		t.Fatalf("ReadTOCEntry: %v", err)
	}

	if got.DataOffset != 1024 {
		t.Errorf("DataOffset = %d, want 1024", got.DataOffset)
	}
	if got.DataLength != 500000 {
		t.Errorf("DataLength = %d, want 500000", got.DataLength)
	}
	if got.DataCompLen != 125000 {
		t.Errorf("DataCompLen = %d, want 125000", got.DataCompLen)
	}
	if got.RowCount != 10000 {
		t.Errorf("RowCount = %d, want 10000", got.RowCount)
	}
}

func TestTOCMultipleEntries(t *testing.T) {
	var buf bytes.Buffer

	entries := []*TOCEntry{
		{DumpID: 1, Tag: "users", Namespace: "public", Desc: "TABLE", Section: SectionPreData, Defn: "CREATE TABLE users..."},
		{DumpID: 2, Tag: "orders", Namespace: "public", Desc: "TABLE", Section: SectionPreData, Defn: "CREATE TABLE orders..."},
		{DumpID: 3, Tag: "users", Namespace: "public", Desc: "TABLE DATA", Section: SectionData, DataOffset: 100, DataLength: 5000, DataCompLen: 1000, RowCount: 50},
		{DumpID: 4, Tag: "orders", Namespace: "public", Desc: "TABLE DATA", Section: SectionData, DataOffset: 1100, DataLength: 8000, DataCompLen: 2000, RowCount: 100},
		{DumpID: 5, Tag: "idx_users_email", Namespace: "public", Desc: "INDEX", Section: SectionPostData, Defn: "CREATE INDEX idx_users_email ON users(email);"},
	}

	for _, e := range entries {
		if err := WriteTOCEntry(&buf, e); err != nil {
			t.Fatalf("WriteTOCEntry %d: %v", e.DumpID, err)
		}
	}

	for i := range entries {
		got, err := ReadTOCEntry(&buf)
		if err != nil {
			t.Fatalf("ReadTOCEntry %d: %v", i, err)
		}
		if got.DumpID != entries[i].DumpID {
			t.Errorf("entry %d: DumpID = %d, want %d", i, got.DumpID, entries[i].DumpID)
		}
		if got.Tag != entries[i].Tag {
			t.Errorf("entry %d: Tag = %q, want %q", i, got.Tag, entries[i].Tag)
		}
		if got.Section != entries[i].Section {
			t.Errorf("entry %d: Section = %d, want %d", i, got.Section, entries[i].Section)
		}
	}
}

func TestFooterRoundTrip(t *testing.T) {
	var buf bytes.Buffer

	footer := &Footer{
		TOCOffset: 123456,
		TOCCount:  42,
		Checksum:  0xDEADBEEF,
	}

	if err := WriteFooter(&buf, footer); err != nil {
		t.Fatalf("WriteFooter: %v", err)
	}

	if buf.Len() != footerSize {
		t.Fatalf("footer size = %d, want %d", buf.Len(), footerSize)
	}

	// ReadFooter needs io.ReadSeeker — wrap in bytes.Reader
	rs := bytes.NewReader(buf.Bytes())
	got, err := ReadFooter(rs)
	if err != nil {
		t.Fatalf("ReadFooter: %v", err)
	}

	if got.TOCOffset != 123456 {
		t.Errorf("TOCOffset = %d, want 123456", got.TOCOffset)
	}
	if got.TOCCount != 42 {
		t.Errorf("TOCCount = %d, want 42", got.TOCCount)
	}
	if got.Checksum != 0xDEADBEEF {
		t.Errorf("Checksum = 0x%X, want 0xDEADBEEF", got.Checksum)
	}
	if got.FooterSize != int32(footerSize) {
		t.Errorf("FooterSize = %d, want %d", got.FooterSize, footerSize)
	}
}

func TestTOCOperations(t *testing.T) {
	toc := NewTOC()

	// Add entries
	id1 := toc.AddEntry(&TOCEntry{Tag: "users", Namespace: "public", Desc: "TABLE", Section: SectionPreData})
	id2 := toc.AddEntry(&TOCEntry{Tag: "orders", Namespace: "public", Desc: "TABLE", Section: SectionPreData})
	id3 := toc.AddEntry(&TOCEntry{Tag: "users", Namespace: "public", Desc: "TABLE DATA", Section: SectionData, DataOffset: 100, DataLength: 5000, DataCompLen: 1000})
	id4 := toc.AddEntry(&TOCEntry{Tag: "orders", Namespace: "public", Desc: "TABLE DATA", Section: SectionData, DataOffset: 1100, DataLength: 8000, DataCompLen: 2000})
	id5 := toc.AddEntry(&TOCEntry{Tag: "idx_users_email", Namespace: "public", Desc: "INDEX", Section: SectionPostData})

	if id1 != 1 || id2 != 2 || id3 != 3 || id4 != 4 || id5 != 5 {
		t.Errorf("IDs: got %d,%d,%d,%d,%d, want 1,2,3,4,5", id1, id2, id3, id4, id5)
	}

	// CountBySection
	pre, data, post := toc.CountBySection()
	if pre != 2 || data != 2 || post != 1 {
		t.Errorf("CountBySection: got %d,%d,%d, want 2,2,1", pre, data, post)
	}

	// GetEntry
	e := toc.GetEntry(3)
	if e == nil || e.Tag != "users" || e.Section != SectionData {
		t.Errorf("GetEntry(3) failed")
	}

	// DataEntries
	dataEntries := toc.DataEntries()
	if len(dataEntries) != 2 {
		t.Errorf("DataEntries: got %d, want 2", len(dataEntries))
	}

	// PreDataEntries
	preEntries := toc.PreDataEntries()
	if len(preEntries) != 2 {
		t.Errorf("PreDataEntries: got %d, want 2", len(preEntries))
	}

	// PostDataEntries
	postEntries := toc.PostDataEntries()
	if len(postEntries) != 1 {
		t.Errorf("PostDataEntries: got %d, want 1", len(postEntries))
	}

	// FilterByTags
	filtered := toc.FilterByTags([]string{"users"})
	if len(filtered) != 2 { // TABLE + TABLE DATA
		t.Errorf("FilterByTags(users): got %d, want 2", len(filtered))
	}

	// SortBySize - largest first
	sorted := toc.SortBySize()
	if len(sorted) != 2 {
		t.Fatalf("SortBySize: got %d, want 2", len(sorted))
	}
	if sorted[0].Tag != "orders" { // 8000 > 5000
		t.Errorf("SortBySize[0] = %q, want orders (larger)", sorted[0].Tag)
	}
}

func TestTopologicalOrder(t *testing.T) {
	toc := NewTOC()

	// orders depends on users (foreign key)
	toc.AddEntry(&TOCEntry{
		Tag: "users", Namespace: "public", Desc: "TABLE DATA",
		Section: SectionData, DataOffset: 100, DataLength: 100, DataCompLen: 50,
	})
	toc.AddEntry(&TOCEntry{
		Tag: "orders", Namespace: "public", Desc: "TABLE DATA",
		Section: SectionData, DataOffset: 200, DataLength: 200, DataCompLen: 100,
		Dependencies: []int32{1}, // depends on users (DumpID 1)
	})

	sorted := toc.TopologicalOrder()
	if len(sorted) != 2 {
		t.Fatalf("TopologicalOrder: got %d entries, want 2", len(sorted))
	}
	if sorted[0].Tag != "users" {
		t.Errorf("first entry = %q, want users (dependency)", sorted[0].Tag)
	}
	if sorted[1].Tag != "orders" {
		t.Errorf("second entry = %q, want orders (dependent)", sorted[1].Tag)
	}
}

func TestFullArchiveRoundTrip(t *testing.T) {
	// Build a complete archive in memory: header + data blocks + TOC + footer
	var archive bytes.Buffer

	// 1. Write header
	hdr := &CustomFormatHeader{
		Compression: CompressNone,
		CompLevel:   0,
		Flags:       0x03,
	}
	copy(hdr.DBName[:], "roundtrip_test")
	if err := WriteHeader(&archive, hdr); err != nil {
		t.Fatalf("WriteHeader: %v", err)
	}

	offset := int64(headerSize)

	// 2. Write some fake data blocks
	tableData1 := []byte("1\tAlice\n2\tBob\n3\tCharlie\n")
	tableData2 := []byte("100\t1\t2025-01-01\n101\t2\t2025-01-02\n")

	archive.Write(tableData1)
	data1Offset := offset
	data1Len := int64(len(tableData1))
	offset += data1Len

	archive.Write(tableData2)
	data2Offset := offset
	data2Len := int64(len(tableData2))
	offset += data2Len

	// 3. Build TOC
	toc := NewTOC()
	toc.AddEntry(&TOCEntry{
		Tag: "users", Namespace: "public", Desc: "TABLE", Section: SectionPreData,
		Defn: "CREATE TABLE public.users (id serial, name text);",
	})
	toc.AddEntry(&TOCEntry{
		Tag: "orders", Namespace: "public", Desc: "TABLE", Section: SectionPreData,
		Defn: "CREATE TABLE public.orders (id serial, user_id int, created_at date);",
	})
	toc.AddEntry(&TOCEntry{
		Tag: "users", Namespace: "public", Desc: "TABLE DATA", Section: SectionData,
		TableName: "public.users", DataOffset: data1Offset, DataLength: data1Len, DataCompLen: data1Len, RowCount: 3,
	})
	toc.AddEntry(&TOCEntry{
		Tag: "orders", Namespace: "public", Desc: "TABLE DATA", Section: SectionData,
		TableName: "public.orders", DataOffset: data2Offset, DataLength: data2Len, DataCompLen: data2Len, RowCount: 2,
		Dependencies: []int32{3},
	})
	toc.AddEntry(&TOCEntry{
		Tag: "idx_users_name", Namespace: "public", Desc: "INDEX", Section: SectionPostData,
		Defn: "CREATE INDEX idx_users_name ON public.users (name);",
	})

	// 4. Write TOC
	tocOffset := offset
	var tocBuf bytes.Buffer
	for _, entry := range toc.Entries {
		if err := WriteTOCEntry(&tocBuf, entry); err != nil {
			t.Fatalf("WriteTOCEntry: %v", err)
		}
	}
	archive.Write(tocBuf.Bytes())

	// 5. Write footer
	footer := &Footer{
		TOCOffset: tocOffset,
		TOCCount:  int32(len(toc.Entries)),
		Checksum:  0,
	}
	if err := WriteFooter(&archive, footer); err != nil {
		t.Fatalf("WriteFooter: %v", err)
	}

	// ═══════════════════════════════════════════════════════════════════
	// NOW READ IT BACK
	// ═══════════════════════════════════════════════════════════════════

	archiveReader := bytes.NewReader(archive.Bytes())

	// Read header
	readHdr, err := ReadHeader(archiveReader)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
	if readHdr.Compression != CompressNone {
		t.Errorf("compression = %d, want %d", readHdr.Compression, CompressNone)
	}

	// Read footer
	readFooter, err := ReadFooter(archiveReader)
	if err != nil {
		t.Fatalf("ReadFooter: %v", err)
	}
	if readFooter.TOCOffset != tocOffset {
		t.Errorf("TOCOffset = %d, want %d", readFooter.TOCOffset, tocOffset)
	}
	if readFooter.TOCCount != 5 {
		t.Errorf("TOCCount = %d, want 5", readFooter.TOCCount)
	}

	// Read TOC
	if _, err := archiveReader.Seek(readFooter.TOCOffset, io.SeekStart); err != nil {
		t.Fatalf("seek to TOC: %v", err)
	}

	readTOC := NewTOC()
	for i := int32(0); i < readFooter.TOCCount; i++ {
		entry, err := ReadTOCEntry(archiveReader)
		if err != nil {
			t.Fatalf("ReadTOCEntry %d: %v", i, err)
		}
		readTOC.Entries = append(readTOC.Entries, entry)
		readTOC.entryMap[entry.DumpID] = entry
	}

	// Verify TOC
	if len(readTOC.Entries) != 5 {
		t.Fatalf("TOC entries = %d, want 5", len(readTOC.Entries))
	}

	pre, data, post := readTOC.CountBySection()
	if pre != 2 || data != 2 || post != 1 {
		t.Errorf("sections: got %d,%d,%d, want 2,2,1", pre, data, post)
	}

	// Read data blocks using offsets from TOC
	dataEntries := readTOC.DataEntries()
	if len(dataEntries) != 2 {
		t.Fatalf("data entries = %d, want 2", len(dataEntries))
	}

	for _, de := range dataEntries {
		if _, err := archiveReader.Seek(de.DataOffset, io.SeekStart); err != nil {
			t.Fatalf("seek to data %s: %v", de.Tag, err)
		}
		readData := make([]byte, de.DataCompLen)
		if _, err := io.ReadFull(archiveReader, readData); err != nil {
			t.Fatalf("read data %s: %v", de.Tag, err)
		}

		switch de.Tag {
		case "users":
			if string(readData) != string(tableData1) {
				t.Errorf("users data mismatch: got %q", string(readData))
			}
		case "orders":
			if string(readData) != string(tableData2) {
				t.Errorf("orders data mismatch: got %q", string(readData))
			}
		}
	}

	// Verify dependency order
	sorted := readTOC.TopologicalOrder()
	if sorted[0].Tag != "users" {
		t.Errorf("topological first = %q, want users", sorted[0].Tag)
	}

	// Verify post-data
	postEntries := readTOC.PostDataEntries()
	if len(postEntries) != 1 || postEntries[0].Tag != "idx_users_name" {
		t.Errorf("post-data entries unexpected")
	}
}

func TestTOCEntryType(t *testing.T) {
	tests := []struct {
		section uint8
		want    string
	}{
		{SectionPreData, "pre-data"},
		{SectionData, "data"},
		{SectionPostData, "post-data"},
		{SectionNone, "other"},
	}

	for _, tt := range tests {
		e := &TOCEntry{Section: tt.section}
		if got := e.TOCEntryType(); got != tt.want {
			t.Errorf("TOCEntryType(%d) = %q, want %q", tt.section, got, tt.want)
		}
	}
}

func TestExtractIdxName(t *testing.T) {
	tests := []struct {
		sql  string
		want string
	}{
		{"CREATE INDEX idx_users_email ON public.users (email);", "idx_users_email"},
		{"CREATE UNIQUE INDEX idx_orders_id ON public.orders (id);", "idx_orders_id"},
		{"SOMETHING ELSE", "unknown_index"},
	}
	for _, tt := range tests {
		if got := extractIdxName(tt.sql); got != tt.want {
			t.Errorf("extractIdxName(%q) = %q, want %q", tt.sql, got, tt.want)
		}
	}
}

func TestExtractConName(t *testing.T) {
	tests := []struct {
		sql  string
		want string
	}{
		{"ALTER TABLE public.orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id);", "fk_user"},
		{"SOMETHING ELSE", "unknown_constraint"},
	}
	for _, tt := range tests {
		if got := extractConName(tt.sql); got != tt.want {
			t.Errorf("extractConName(%q) = %q, want %q", tt.sql, got, tt.want)
		}
	}
}

func TestCompressionName(t *testing.T) {
	tests := []struct {
		c    uint8
		want string
	}{
		{CompressNone, "none"},
		{CompressGzip, "gzip"},
		{CompressZstd, "zstd"},
		{CompressLZ4, "lz4"},
		{255, "unknown"},
	}
	for _, tt := range tests {
		if got := compressionName(tt.c); got != tt.want {
			t.Errorf("compressionName(%d) = %q, want %q", tt.c, got, tt.want)
		}
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		b    int64
		want string
	}{
		{500, "500 B"},
		{1500, "1.5 KB"},
		{1500000, "1.4 MB"},
		{1500000000, "1.4 GB"},
	}
	for _, tt := range tests {
		if got := formatBytes(tt.b); got != tt.want {
			t.Errorf("formatBytes(%d) = %q, want %q", tt.b, got, tt.want)
		}
	}
}

func TestSectionLabel(t *testing.T) {
	tests := []struct {
		s    uint8
		want string
	}{
		{SectionPreData, "PRE-DATA"},
		{SectionData, "DATA"},
		{SectionPostData, "POST-DATA"},
		{SectionNone, "NONE"},
	}
	for _, tt := range tests {
		if got := sectionLabel(tt.s); got != tt.want {
			t.Errorf("sectionLabel(%d) = %q, want %q", tt.s, got, tt.want)
		}
	}
}

func TestFilterBySection(t *testing.T) {
	toc := NewTOC()
	toc.AddEntry(&TOCEntry{Tag: "t1", Section: SectionPreData})
	toc.AddEntry(&TOCEntry{Tag: "t2", Section: SectionData, DataOffset: 1, DataLength: 1, DataCompLen: 1})
	toc.AddEntry(&TOCEntry{Tag: "t3", Section: SectionPostData})
	toc.AddEntry(&TOCEntry{Tag: "t4", Section: SectionPreData})

	pre := toc.FilterBySection(SectionPreData)
	if len(pre) != 2 {
		t.Errorf("FilterBySection(PreData) = %d, want 2", len(pre))
	}
	data := toc.FilterBySection(SectionData)
	if len(data) != 1 {
		t.Errorf("FilterBySection(Data) = %d, want 1", len(data))
	}
	post := toc.FilterBySection(SectionPostData)
	if len(post) != 1 {
		t.Errorf("FilterBySection(PostData) = %d, want 1", len(post))
	}
}

func TestShouldIncludeEntry(t *testing.T) {
	reader := &CustomFormatReader{}

	entry := &TOCEntry{Tag: "users", Namespace: "public", TableName: "public.users"}

	// No filters
	if !reader.shouldIncludeEntry(entry, &CustomFormatRestoreOptions{}) {
		t.Error("should include with no filters")
	}

	// Include filter - match
	if !reader.shouldIncludeEntry(entry, &CustomFormatRestoreOptions{IncludeTables: []string{"users"}}) {
		t.Error("should include by tag name")
	}

	// Include filter - no match
	if reader.shouldIncludeEntry(entry, &CustomFormatRestoreOptions{IncludeTables: []string{"orders"}}) {
		t.Error("should exclude when not in include list")
	}

	// Exclude filter - match
	if reader.shouldIncludeEntry(entry, &CustomFormatRestoreOptions{ExcludeTables: []string{"users"}}) {
		t.Error("should exclude when in exclude list")
	}

	// Exclude filter - no match
	if !reader.shouldIncludeEntry(entry, &CustomFormatRestoreOptions{ExcludeTables: []string{"orders"}}) {
		t.Error("should include when not in exclude list")
	}
}

func TestStringRoundTrips(t *testing.T) {
	tests := []string{
		"",
		"hello",
		"SELECT * FROM \"public\".\"users\" WHERE name = 'O\\'Brien'",
		"Unicode: 日本語テスト ñ ü ö",
		// Long string
		string(make([]byte, 10000)),
	}

	for _, s := range tests {
		var buf bytes.Buffer
		if err := writeString(&buf, s); err != nil {
			t.Fatalf("writeString(%q): %v", s[:min(len(s), 20)], err)
		}
		got, err := readString(&buf)
		if err != nil {
			t.Fatalf("readString: %v", err)
		}
		if got != s {
			t.Errorf("string round trip failed for len=%d", len(s))
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
