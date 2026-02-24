package performance

import (
	"bytes"
	"context"
	"io"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestBufferPool(t *testing.T) {
	pool := NewBufferPool()

	t.Run("SmallBuffer", func(t *testing.T) {
		buf := pool.GetSmall()
		if len(*buf) != SmallBufferSize {
			t.Errorf("expected small buffer size %d, got %d", SmallBufferSize, len(*buf))
		}
		pool.PutSmall(buf)
	})

	t.Run("MediumBuffer", func(t *testing.T) {
		buf := pool.GetMedium()
		if len(*buf) != MediumBufferSize {
			t.Errorf("expected medium buffer size %d, got %d", MediumBufferSize, len(*buf))
		}
		pool.PutMedium(buf)
	})

	t.Run("LargeBuffer", func(t *testing.T) {
		buf := pool.GetLarge()
		if len(*buf) != LargeBufferSize {
			t.Errorf("expected large buffer size %d, got %d", LargeBufferSize, len(*buf))
		}
		pool.PutLarge(buf)
	})

	t.Run("HugeBuffer", func(t *testing.T) {
		buf := pool.GetHuge()
		if len(*buf) != HugeBufferSize {
			t.Errorf("expected huge buffer size %d, got %d", HugeBufferSize, len(*buf))
		}
		pool.PutHuge(buf)
	})

	t.Run("ConcurrentAccess", func(t *testing.T) {
		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				buf := pool.GetLarge()
				time.Sleep(time.Millisecond)
				pool.PutLarge(buf)
			}()
		}
		wg.Wait()
	})
}

func TestOptimizedCopy(t *testing.T) {
	testData := make([]byte, 10*1024*1024) // 10MB
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	t.Run("BasicCopy", func(t *testing.T) {
		src := bytes.NewReader(testData)
		dst := &bytes.Buffer{}

		n, err := OptimizedCopy(context.Background(), dst, src)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if n != int64(len(testData)) {
			t.Errorf("expected to copy %d bytes, copied %d", len(testData), n)
		}
		if !bytes.Equal(dst.Bytes(), testData) {
			t.Error("copied data does not match source")
		}
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		src := &slowReader{data: testData}
		dst := &bytes.Buffer{}

		ctx, cancel := context.WithCancel(context.Background())

		// Cancel after a short delay
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()

		_, err := OptimizedCopy(ctx, dst, src)
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	})
}

// slowReader simulates a slow reader for testing context cancellation
type slowReader struct {
	data   []byte
	offset int
}

func (r *slowReader) Read(p []byte) (int, error) {
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}

	time.Sleep(5 * time.Millisecond)

	n := copy(p, r.data[r.offset:])
	r.offset += n
	return n, nil
}

func TestHighThroughputCopy(t *testing.T) {
	testData := make([]byte, 50*1024*1024) // 50MB
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	src := bytes.NewReader(testData)
	dst := &bytes.Buffer{}

	n, err := HighThroughputCopy(context.Background(), dst, src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != int64(len(testData)) {
		t.Errorf("expected to copy %d bytes, copied %d", len(testData), n)
	}
}

func TestMetricsCollector(t *testing.T) {
	mc := NewMetricsCollector()
	mc.Start()

	// Simulate some work
	mc.RecordRead(1024)
	mc.RecordWrite(512)
	mc.RecordCompression(100 * time.Millisecond)
	mc.RecordIO(50 * time.Millisecond)

	time.Sleep(50 * time.Millisecond)

	result := mc.Stop("test", "dump", 1024)

	if result.Name != "test" {
		t.Errorf("expected name 'test', got %s", result.Name)
	}
	if result.Operation != "dump" {
		t.Errorf("expected operation 'dump', got %s", result.Operation)
	}
	if result.DataSizeBytes != 1024 {
		t.Errorf("expected data size 1024, got %d", result.DataSizeBytes)
	}
	if result.ReadBytes != 1024 {
		t.Errorf("expected read bytes 1024, got %d", result.ReadBytes)
	}
	if result.WriteBytes != 512 {
		t.Errorf("expected write bytes 512, got %d", result.WriteBytes)
	}
}

func TestBytesBufferPool(t *testing.T) {
	pool := NewBytesBufferPool()

	buf := pool.Get()
	buf.WriteString("test data")

	pool.Put(buf)

	// Get another buffer - should be reset
	buf2 := pool.Get()
	if buf2.Len() != 0 {
		t.Error("buffer should be reset after Put")
	}
	pool.Put(buf2)
}

func TestPipelineStage(t *testing.T) {
	// Simple passthrough process
	passthrough := func(ctx context.Context, chunk *ChunkData) (*ChunkData, error) {
		return chunk, nil
	}

	stage := NewPipelineStage("test", 2, 4, passthrough)
	stage.Start()

	// Send some chunks
	for i := 0; i < 10; i++ {
		chunk := &ChunkData{
			Data:     []byte("test data"),
			Size:     9,
			Sequence: int64(i),
		}
		stage.Input() <- chunk
	}

	// Receive results
	received := 0
	timeout := time.After(1 * time.Second)

loop:
	for received < 10 {
		select {
		case <-stage.Output():
			received++
		case <-timeout:
			break loop
		}
	}

	stage.Stop()

	if received != 10 {
		t.Errorf("expected 10 chunks, received %d", received)
	}

	metrics := stage.Metrics()
	if metrics.ChunksProcessed.Load() != 10 {
		t.Errorf("expected 10 chunks processed, got %d", metrics.ChunksProcessed.Load())
	}
}

// Benchmarks

func BenchmarkBufferPoolSmall(b *testing.B) {
	pool := NewBufferPool()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf := pool.GetSmall()
		pool.PutSmall(buf)
	}
}

func BenchmarkBufferPoolLarge(b *testing.B) {
	pool := NewBufferPool()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf := pool.GetLarge()
		pool.PutLarge(buf)
	}
}

func BenchmarkBufferPoolConcurrent(b *testing.B) {
	pool := NewBufferPool()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := pool.GetLarge()
			pool.PutLarge(buf)
		}
	})
}

func BenchmarkBufferAllocation(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf := make([]byte, LargeBufferSize)
		_ = buf
	}
}

func BenchmarkOptimizedCopy(b *testing.B) {
	testData := make([]byte, 10*1024*1024) // 10MB
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	b.SetBytes(int64(len(testData)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		src := bytes.NewReader(testData)
		dst := &bytes.Buffer{}
		_, _ = OptimizedCopy(context.Background(), dst, src)
	}
}

func BenchmarkHighThroughputCopy(b *testing.B) {
	testData := make([]byte, 10*1024*1024) // 10MB
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	b.SetBytes(int64(len(testData)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		src := bytes.NewReader(testData)
		dst := &bytes.Buffer{}
		_, _ = HighThroughputCopy(context.Background(), dst, src)
	}
}

func BenchmarkStandardCopy(b *testing.B) {
	testData := make([]byte, 10*1024*1024) // 10MB
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	b.SetBytes(int64(len(testData)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		src := bytes.NewReader(testData)
		dst := &bytes.Buffer{}
		_, _ = io.Copy(dst, src)
	}
}

func BenchmarkCaptureMemStats(b *testing.B) {
	for i := 0; i < b.N; i++ {
		CaptureMemStats()
	}
}

func BenchmarkMetricsCollector(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		mc := NewMetricsCollector()
		mc.Start()
		mc.RecordRead(1024)
		mc.RecordWrite(512)
		mc.Stop("bench", "dump", 1024)
	}
}

func BenchmarkPipelineStage(b *testing.B) {
	passthrough := func(ctx context.Context, chunk *ChunkData) (*ChunkData, error) {
		return chunk, nil
	}

	stage := NewPipelineStage("bench", runtime.NumCPU(), 16, passthrough)
	stage.Start()
	defer stage.Stop()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		chunk := &ChunkData{
			Data:     make([]byte, 1024),
			Size:     1024,
			Sequence: int64(i),
		}
		stage.Input() <- chunk
		<-stage.Output()
	}
}
