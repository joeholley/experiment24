// Tries to use the guidelines at https://cloud.google.com/apis/design for the gRPC API where possible.
package store

// An enum for the type of operations that the replication queue can process
const (
	Ticket = iota
	Activate
	Deactivate
	Assign
)

// Every change to the state of the tickets in om-core is modelled as a StateUpdate.
type StateUpdate struct {
	Cmd   int    // The operation this update contains
	Key   string // The key to update
	Value string // The value to associate with this key (if applicable)
}

// Results of changes to the state of the cache. State replication batches
// updates as much as possible, and every update generates a StateResponse that
// can be sent back to the underlying caller, regardless of what other
// operations were part of batch.
//
// If err is nil, result contains the replication id assigned to the update by the
// state storage implementation (in redis, this will be a stream event ID, for
// example). For most end users, their only exposure to this is the response
// from a CreateTicket call - this is how that call gets back the ticket ID
// from state storage. It may also used in internal implementations to track
// which updates have been applied to the local replicated ticket cache.
//
// If err is not nil, result contains the key of the StateUpdate that failed,
// so the calling function can determine what failed.
type StateResponse struct {
	Result string
	Err    error
}

// The core gRPC server instantiates a replicatedTicketCache on startup, and specifies
// how it wants to replicate om-core state by instantiating a StateReplicator
// that conforms to this interface.
type StateReplicator interface {
	GetUpdates() []*StateUpdate
	SendUpdates([]*StateUpdate) []*StateResponse
	GetReplIdValidationRegex() string
}
