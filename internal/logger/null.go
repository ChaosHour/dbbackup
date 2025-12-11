package logger

// NullLogger is a logger that discards all output (useful for testing)
type NullLogger struct{}

// NewNullLogger creates a new null logger
func NewNullLogger() *NullLogger {
	return &NullLogger{}
}

func (l *NullLogger) Info(msg string, args ...any)  {}
func (l *NullLogger) Warn(msg string, args ...any)  {}
func (l *NullLogger) Error(msg string, args ...any) {}
func (l *NullLogger) Debug(msg string, args ...any) {}
func (l *NullLogger) Time(msg string, args ...any)  {}

// WithField returns the same NullLogger (no-op for null logger)
func (l *NullLogger) WithField(key string, value interface{}) Logger {
	return l
}

// WithFields returns the same NullLogger (no-op for null logger)
func (l *NullLogger) WithFields(fields map[string]interface{}) Logger {
	return l
}

func (l *NullLogger) StartOperation(name string) OperationLogger {
	return &nullOperation{}
}

type nullOperation struct{}

func (o *nullOperation) Update(msg string, args ...any)   {}
func (o *nullOperation) Complete(msg string, args ...any) {}
func (o *nullOperation) Fail(msg string, args ...any)     {}
