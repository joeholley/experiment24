// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package config contains convenience functions for reading and managing viper configs.
package config

import (
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"open-match.dev/core/internal/logging"
)

// readConfig reads Open Match configuration from the environment.
//
// readConfig sets default config values, and configures Viper to read from the environment.
// The AutomaticEnv function of Viper reads values from env vars, but critically,
// ONLY FOR KEYS IT ALREADY HAS A DEFAULT VALUE FOR. If you've defined a new env var and you're
// trying to access it as a config value in OM, you MUST specify a default for it here, or
// Viper will NOT read the value you set in the env var!
//
// By convention:
// - all Open Match configuration keys should be ALL CAPS and start with "OM_"
// - all keys with time duration values should end in units, for example "_SECS" or "_MS"
// If both a default and env var value exist for the same variable, the env var value wins.
func Read() *viper.Viper {
	cfg := viper.New()

	// Logging defaults
	cfg.SetDefault("OM_LOGGING_FORMAT", "json")
	cfg.SetDefault("OM_LOGGING_LEVEL", "info")

	// Where the OM state is stored: 'memory' should not be used in production! It makes this
	// instance into an island that does not receive/send to/from other om-core instances.
	cfg.SetDefault("OM_STATE_STORAGE_TYPE", "memory") // 'Redis' to use the redis replication feature.

	// Set to false to wait until after all ticket deactivation requests for tickets in a match
	// have been sent to the replication queue before returning the match to the InvokeMatchMakingFunction() client.
	// TODO: validate this has the expected effect when using redis
	cfg.SetDefault("OM_MATCH_TICKET_DEACTIVATION_WAIT", false)

	// Maximum number of updates allowed in activate/deactivate/assignment gRPC calls.
	// Can be configured but must be an int32 value.
	cfg.SetDefault("OM_MAX_STATE_UPDATES_PER_CALL", 500)

	// Maximum
	cfg.SetDefault("OM_MMF_TIMEOUT_SECS", 500)
	// By default, configure OM to read and write to the same redis instance. When moving to production,
	// you may want to direct reads to a replica and writes to the Redis master, or even load-balance read
	// requests across several read replicas. See documentation for more details.
	//
	// Redis connection pool configuration
	cfg.SetDefault("OM_REDIS_POOL_MAX_IDLE", 500)
	cfg.SetDefault("OM_REDIS_POOL_MAX_ACTIVE", 500)
	cfg.SetDefault("OM_REDIS_POOL_IDLE_TIMEOUT", 0)

	// Redis write configuration
	cfg.SetDefault("OM_REDIS_WRITE_HOST", "192.168.1.100")
	cfg.SetDefault("OM_REDIS_WRITE_PORT", 6379)
	cfg.SetDefault("OM_REDIS_WRITE_USER", "default")
	cfg.SetDefault("OM_REDIS_WRITE_PASSWORD", "om-redis")
	cfg.SetDefault("OM_REDIS_PIPELINE_MAX_QUEUE_THRESHOLD", 50) // In number of update operations
	cfg.SetDefault("OM_REDIS_PIPELINE_WAIT_TIMEOUT_MS", 500)    // In milliseconds
	cfg.SetDefault("OM_TICKET_TTL_SECS", 600)                   // In seconds

	// How long assignments will be retained AFTER ticket expiration
	// Assignments features are DEPRECATED
	cfg.SetDefault("OM_ASSIGNMENT_TTL_SECS", 600) // In seconds

	// Redis read configuration
	cfg.SetDefault("OM_REDIS_READ_HOST", "192.168.1.100")
	cfg.SetDefault("OM_REDIS_READ_PORT", 6379)
	cfg.SetDefault("OM_REDIS_READ_USER", "default")
	cfg.SetDefault("OM_REDIS_READ_PASSWORD", "om-redis")
	cfg.SetDefault("OM_REDIS_REPLICATION_WAIT_TIMEOUT_MS", 1500)       // In milliseconds
	cfg.SetDefault("OM_REDIS_REPLICATION_MAX_UPDATES_PER_POLL", 10000) // In number of update operations

	// By default, OM tries to talk to the OTEL collector on localhost. This works when doing local
	// development, and also when running a sidecar OTEL collector in a k8s pod or serverless environment.
	cfg.SetDefault("OM_OTEL_COLLECTOR_HOST", "localhost")
	cfg.SetDefault("OM_OTEL_COLLECTOR_PORT", 4317)

	// knative env vars https://cloud.google.com/run/docs/container-contract#env-vars
	cfg.SetDefault("PORT", 8080)
	cfg.SetDefault("K_SERVICE", "open_match_core")
	cfg.SetDefault("K_REVISION", "open_match_core_rev.1")
	cfg.SetDefault("K_CONFIGURATION", "open_match_core_cfg")

	// developer options
	cfg.SetDefault("OM_VERBOSE", false)

	// Override default values with those from the environment variables of the same name.
	cfg.AutomaticEnv()

	// DEBUG: dump cfg vars to the log
	logging.ConfigureLogging(cfg)
	logger := logrus.WithFields(logrus.Fields{
		"app":       "open_match",
		"component": "internal.config",
	})
	logger.Debug("configuration:")
	if cfg.GetString("OM_LOGGING_LEVEL") == "debug" {
		for key, value := range cfg.AllSettings() {
			logger.Debugf("  %v: %v", key, value)
		}
	}

	return cfg
}
