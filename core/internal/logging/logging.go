// Copyright 2019 Google LLC
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

// Package logging configures the Logrus logging library.
package logging

import (
	"strings"

	stackdriver "github.com/TV4/logrus-stackdriver-formatter"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// ConfigureLogging sets up open match logrus instance using the open match config env vars
//   - log line format (text, json[default], or stackdriver)
//   - min log level to include (debug, info [default], warn, error, fatal, panic)
func ConfigureLogging(cfg *viper.Viper) {
	logrus.SetFormatter(newFormatter(cfg.GetString("OM_LOGGING_FORMAT")))
	level := toLevel(cfg.GetString("OM_LOGGING_LEVEL"))
	logrus.SetLevel(level)
	if isDebugLevel(level) {
		logrus.Warn("Trace logging level configured. Not recommended for production!")
	}
}

func newFormatter(formatter string) logrus.Formatter {
	switch strings.ToLower(formatter) {
	case "stackdriver":
		return stackdriver.NewFormatter()
	case "json":
		return &logrus.JSONFormatter{}
	}
	return &logrus.TextFormatter{}
}

func toLevel(level string) logrus.Level {
	switch strings.ToLower(level) {
	case "trace":
		return logrus.TraceLevel
	case "debug":
		return logrus.DebugLevel
	case "warn":
		fallthrough
	case "warning":
		return logrus.WarnLevel
	case "error":
		return logrus.ErrorLevel
	case "fatal":
		return logrus.FatalLevel
	case "panic":
		return logrus.PanicLevel
	}
	return logrus.InfoLevel
}

// IsDebugEnabled returns true if the logging level is debug or more granular.
func IsDebugEnabled(cfg *viper.Viper) bool {
	return IsDebugLevel(cfg.GetString("logging.level"))
}

// IsDebugLevel returns true if the logging level is debug or more granular.
func IsDebugLevel(level string) bool {
	return isDebugLevel(toLevel(level))
}

func isDebugLevel(level logrus.Level) bool {
	switch level {
	case logrus.TraceLevel:
		fallthrough
	case logrus.DebugLevel:
		return true
	}
	return false
}