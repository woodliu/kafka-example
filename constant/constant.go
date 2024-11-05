package constant

// Define custom key types to avoid key collisions
type ContextKey string

const (
	OperationID ContextKey = "operationID" // For tracking, debugging
	OpUserID    ContextKey = "opUserID"    // For indentifying user accross micro services
)
