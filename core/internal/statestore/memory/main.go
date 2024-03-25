package memoryReplicator

import (
	"fmt"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	store "open-match.dev/core/internal/statestore/datatypes"
)

var (
	logger = logrus.WithFields(logrus.Fields{
		"app":        "open_match",
		"component":  "statestore",
		"replicator": "memory",
	})
)

// Local, in-memory state storage.  This mocks a subset of the
// Redis Streams functionality to provide the same surface area used by om-core.
// Used for tests and local development. NOT RECOMMENDED FOR PRODUCTION
type memoryReplicator struct {
	cfg       *viper.Viper
	replChan  chan *store.StateUpdate
	replTS    time.Time
	replCount int
}

func New(cfg *viper.Viper) *memoryReplicator {
	return &memoryReplicator{
		replChan:  make(chan *store.StateUpdate),
		replTS:    time.Now(),
		replCount: 0,
		cfg:       cfg,
	}
}

// GetReplIdValidationRegex returns a raw string representing a regular
// expression that can be used to match replication id strings.
func (rc *memoryReplicator) GetReplIdValidationRegex() string {
	return `\d*-\d*`
}

// GetUpdates mocks how the statestore/redis module processes a Redis Stream XRANGE command.
// https://redis.io/docs/data-types/streams/#querying-by-range-xrange-and-xrevrange
func (rc *memoryReplicator) GetUpdates() (out []*store.StateUpdate) {
	logger := logrus.WithFields(logrus.Fields{
		"direction": "getUpdates",
	})

	// Timeout
	timeout := time.After(time.Millisecond * time.Duration(rc.cfg.GetInt("OM_REDIS_PIPELINE_WAIT_TIMEOUT_MS")))

	// Local vars
	var thisUpdate *store.StateUpdate
	more := true

	for more {
		select {
		case thisUpdate, more = <-rc.replChan:
			out = append(out, thisUpdate)
		case <-timeout:
			more = false
		}
	}

	logger.Debugf("read %v updates from state storage, pending application to cache", len(out))
	return out
}

// SendUpdates mocks how the statestore/redis module processes a Redis Stream XADD command.
// https://redis.io/docs/data-types/streams/#streams-basics
func (rc *memoryReplicator) SendUpdates(updates []*store.StateUpdate) []*store.StateResponse {
	logger := logrus.WithFields(logrus.Fields{
		"direction": "sendUpdates",
	})

	out := make([]*store.StateResponse, 0)
	for _, up := range updates {
		replId := rc.getReplId()
		// Creating a new ticket generates a new ID in redis
		// so mock that here.
		if up.Cmd == store.Ticket {
			up.Key = replId
		}
		rc.replChan <- up
		out = append(out, &store.StateResponse{Result: replId, Err: nil})
	}
	logger.Debugf("%v updates written to state storage for replication", len(updates))

	return out
}

// GetReplId mocks how Redis Streams generate entry IDs
// https://redis.io/docs/data-types/streams/#entry-ids
func (rc *memoryReplicator) getReplId() string {
	if time.Now().Unix() == rc.replTS.Unix() {
		rc.replCount += 1
	} else {
		rc.replTS = time.Now()
		rc.replCount = 0
	}
	id := fmt.Sprintf("%v-%v", strconv.FormatInt(rc.replTS.Unix(), 10), rc.replCount)
	return id
}
