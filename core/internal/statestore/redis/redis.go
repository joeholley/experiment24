// redisReplicator is an implementation of open match state storage replication
// using redis streams as the mechanism.
//
// A few notes about its design:
//   - All updates should be modelled as a single redis command that adds one item to the stream.
//     Each addition to the stream gets its own stream ID from redis, and putting multiple state
//     updates into one stream addition isn't accounted for in this design. It could have unforseen
//     consequences.
package redisReplicator

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/gomodule/redigo/redis"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	store "open-match.dev/core/internal/statestore/datatypes"
)

var (
	logger = logrus.WithFields(logrus.Fields{
		"app":        "open_match",
		"component":  "statestore",
		"replicator": "redis",
	})
)

type redisReplicator struct {
	this      *store.StateUpdate
	rConnPool *redis.Pool
	wConnPool *redis.Pool
	cfg       *viper.Viper
	replId    string
}

func New(cfg *viper.Viper) *redisReplicator {
	return &redisReplicator{
		replId: "0-0",
		cfg:    cfg,
		rConnPool: &redis.Pool{ // Redis read pool
			MaxIdle:     cfg.GetInt("OM_REDIS_POOL_MAX_IDLE"),
			MaxActive:   cfg.GetInt("OM_REDIS_POOL_MAX_ACTIVE"),
			IdleTimeout: cfg.GetDuration("OM_REDIS_POOL_IDLE_TIMEOUT"),
			Wait:        true,
			TestOnBorrow: func(c redis.Conn, lastUsed time.Time) error {
				// Assume the connection is valid if it was used in 15 sec.
				if time.Since(lastUsed) < 15*time.Second {
					return nil
				}

				_, err := c.Do("PING")
				return err
			},
			Dial: func() (redis.Conn, error) {
				return redis.Dial("tcp",
					fmt.Sprintf("%s:%s", cfg.GetString("OM_REDIS_READ_HOST"), cfg.GetString("OM_REDIS_READ_PORT")),
					redis.DialUsername(cfg.GetString("OM_REDIS_READ_USER")),
					redis.DialPassword(cfg.GetString("OM_REDIS_READ_PASSWORD")),
					redis.DialConnectTimeout(cfg.GetDuration("OM_REDIS_POOL_IDLE_TIMEOUT")),
					redis.DialReadTimeout(cfg.GetDuration("OM_REDIS_POOL_IDLE_TIMEOUT")),
				)
			},
		},
		wConnPool: &redis.Pool{ // Redis write pool
			MaxIdle:     cfg.GetInt("OM_REDIS_POOL_MAX_IDLE"),
			MaxActive:   cfg.GetInt("OM_REDIS_POOL_MAX_ACTIVE"),
			IdleTimeout: cfg.GetDuration("OM_REDIS_POOL_IDLE_TIMEOUT"),
			Wait:        true,
			TestOnBorrow: func(c redis.Conn, lastUsed time.Time) error {
				// Assume the connection is valid if it was used in 15 sec.
				if time.Since(lastUsed) < 15*time.Second {
					return nil
				}

				_, err := c.Do("PING")
				return err
			},
			Dial: func() (redis.Conn, error) {
				return redis.Dial("tcp",
					fmt.Sprintf("%s:%s", cfg.GetString("OM_REDIS_WRITE_HOST"), cfg.GetString("OM_REDIS_WRITE_PORT")),
					redis.DialUsername(cfg.GetString("OM_REDIS_WRITE_USER")),
					redis.DialPassword(cfg.GetString("OM_REDIS_WRITE_PASSWORD")),
					redis.DialConnectTimeout(cfg.GetDuration("OM_REDIS_POOL_IDLE_TIMEOUT")),
					redis.DialReadTimeout(cfg.GetDuration("OM_REDIS_POOL_IDLE_TIMEOUT")),
				)
			},
		},
	}
}

// sendUpdates accepts an array of state update structs and writes them to data storage, to be
// replicated to all clients (e.g. other instances of om-core).
// When using redis, these will be pipelined together as a batch update to improve performance.
// In addition to the updates sent to the function, it always ends every batch with an XTRIM
// command to delete all updates older than the ttl configured in the environment variable
// OM_TICKET_TTL_SECS.
func (rr *redisReplicator) SendUpdates(updates []*store.StateUpdate) []*store.StateResponse {
	logger := logrus.WithFields(logrus.Fields{
		"app":       "open_match",
		"component": "redisReplicator.sendUpdates",
	})

	var rConn redis.Conn
	const redisCmd = "XADD"
	out := make([]*store.StateResponse, len(updates))

	// Process all the requested redis commands
	for i, update := range updates {
		out[i] = &store.StateResponse{Result: "", Err: nil}
		redisArgs := make([]interface{}, 0)
		redisArgs = append(redisArgs, "om-replication", "*")
		switch update.Cmd {
		case store.Ticket:
			// Validate input
			if update.Value == "" {
				out[i].Err = errors.New("No ticket data")
				continue
			}
			redisArgs = append(redisArgs, "ticket")
			redisArgs = append(redisArgs, update.Value)
		case store.Activate:
			// Validate input
			if update.Key == "" {
				out[i].Err = errors.New("Missing ticket key")
				continue
			}
			redisArgs = append(redisArgs, "activate")
			redisArgs = append(redisArgs, update.Key)
		case store.Deactivate:
			// Validate input
			if update.Key == "" {
				out[i].Err = errors.New("Missing ticket key")
				continue
			}
			redisArgs = append(redisArgs, "deactivate")
			redisArgs = append(redisArgs, update.Key)
		case store.Assign:
			// TODO: decide if we want multiple assignments in one redis record
			// Validate input
			if update.Key == "" {
				out[i].Err = errors.New("Missing ticket key")
				continue
			}
			if update.Value == "" {
				out[i].Err = errors.New("Missing assignment")
				continue
			}
			redisArgs = append(redisArgs, "assign")
			redisArgs = append(redisArgs, update.Key)
			redisArgs = append(redisArgs, "connection")
			redisArgs = append(redisArgs, update.Value)
		default:
			// Something has gone seriously wrong
			out[i].Err = errors.New("Invalid input")
			continue
		}

		// Send command to redis
		logger.Debugf("Executing redis command: %v %v", redisCmd, redisArgs)
		rConn := rr.wConnPool.Get()
		rConn.Send(redisCmd, redisArgs...)
	}

	// Append command to remove expired entries
	expirationThresh := strconv.FormatInt(time.Now().Unix()-rr.cfg.GetInt64("OM_TICKET_TTL_SECS"), 10)
	logger.Debugf("Executing redis command: XTRIM om-replication MINID %v", expirationThresh)
	rConn.Send("XTRIM", "om-replication", "MINID", expirationThresh)
	//sr.redisConn.Send("XTRIM", "om-replication", "MINID", "1707799344834-1")

	// Send pipelined commands, get results
	r, err := rConn.Do("")
	if err != nil {
		logger.Errorf("Do error: %v", err)
	}

	// Last command we sent was the XTRIM above, so read all results but that final one into
	// the array of strings to return from this function
	for index := 0; index < len(r.([]interface{}))-1; index++ {
		// First, make sure this was a valid update
		if out[index].Err == nil {
			t, err := redis.String(r.([]interface{})[index], err)
			if err != nil {
				out[index].Err = fmt.Errorf("Redis output string conversion error: %w", err)
				t = ""
			}
			out[index].Result = t
			fmt.Printf(" %v - found string: %v\n", index, t)
		} else {
			// Error, the update didn't have all the required fields so it was never sent to redis.
			// Return the error code, and the result is the key that generated the error.
			fmt.Printf(" %v - had error\n", index)
			out[index].Result = updates[index].Key
		}
	}

	// Last result from redis is a count of number of entries we removed with the XTRIM command.
	expiredCount, err := redis.Int64(r.([]interface{})[len(r.([]interface{}))-1], err)
	if err != nil {
		logger.Errorf("Redis output int64 conversion error: %v", err)
	}
	if expiredCount > 0 {
		logger.Debugf("Expired %v Redis entries due to configured OM_TICKET_TTL_SECS value of %v (expiration threshold timestamp %v)", expiredCount, rr.cfg.GetInt64("OM_TICKET_TTL_SECS"), expirationThresh)
	}

	return out
}

// getUpdates performs a blocking read on state (with a configurable timeout read from
// the environment variable OM_REPLICATION_WAIT_TIMEOUT_MS) to receive
// all update structs that have been sent to the replicator since the last getUpdates request.
// They are returned in an array, and om-core is required to apply them
// as events in the order they were originally received.
func (rr *redisReplicator) GetUpdates() []*store.StateUpdate {
	logger := logrus.WithFields(logrus.Fields{
		"app":       "open_match",
		"component": "redisReplicator.getUpdates",
	})

	// Output array of stateUpdates.
	out := make([]*store.StateUpdate, 0)

	// Generate redis command to retrieve updates
	redisCmd := "XREAD"
	redisArgs := make([]interface{}, 0)
	// Max num of updates to get in one go
	redisArgs = append(redisArgs, "COUNT", rr.cfg.GetString("OM_REDIS_REPLICATION_MAX_UPDATES_PER_POLL"))
	// Timeout when waiting for stream updates
	redisArgs = append(redisArgs, "BLOCK", rr.cfg.GetString("OM_REDIS_REPLICATION_WAIT_TIMEOUT_MS"))
	// Replication stream name
	redisArgs = append(redisArgs, "STREAMS", "om-replication")
	// Last ID read from this stream. Initialized to 0-0 (beginning of the stream) on startup.
	redisArgs = append(redisArgs, rr.replId)
	logger.Debugf("Executing redis command: %v %v", redisCmd, redisArgs)

	rConn := rr.rConnPool.Get()
	data, err := rConn.Do(redisCmd, redisArgs...)
	if err != nil {
		logger.Errorf("Redis error: %v", err)
	}
	spew.Dump(data)

	// Redigo module returns nil for the data when it saw no update to the redis stream before the BLOCK
	// timeout was reached. In that case, just return gracefully.
	if data != nil {
		// We're only using one stream for replication, so the
		// data is down a couple of nested array levels in the response.
		replStream := data.([]interface{})[0].([]interface{})[1].([]interface{})
		for _, v := range replStream {
			replId, err := redis.String(v.([]interface{})[0], nil)
			if err != nil {
				logger.Error(err)
			}
			thisUpdate := &store.StateUpdate{}
			//fmt.Println(replId)

			// Update type/key/value data
			y, err := redis.Strings(v.([]interface{})[1], nil)
			if err != nil {
				logger.Error(err)
			}
			switch y[0] {
			case "ticket":
				thisUpdate.Cmd = store.Ticket
				thisUpdate.Key = replId
				thisUpdate.Value = y[1] // Only argument for a ticket is the ticket PB
			case "activate":
				thisUpdate.Cmd = store.Activate
				thisUpdate.Key = y[1] // Only argument for a ticket activation is the ticket's ID
			case "deactivate":
				thisUpdate.Cmd = store.Deactivate
				thisUpdate.Key = y[1] // Only argument for a ticket deactivation is the ticket's ID
			case "assign":
				thisUpdate.Cmd = store.Assign
				thisUpdate.Key = y[1]   // ticket's ID
				thisUpdate.Value = y[3] // assignment
			}

			// Populated all the fields without an error
			out = append(out, thisUpdate)

			// update the current replId to indicate this update was processed
			rr.replId = replId
		}
	}
	return out
}

// getReplIdValidationRegex returns a raw string representing a regular
// expression that can be used to match replication id strings.
// https://redis.io/docs/data-types/streams/#entry-ids
func (rr *redisReplicator) GetReplIdValidationRegex() string {
	return `\d*-\d*`
}
