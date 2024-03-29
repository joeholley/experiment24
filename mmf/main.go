// Copyright 2024 Google LLC
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

// This sample is a reference to demonstrate serving a golang matchmaking
// function using gRPC, and can be used as a starting point for your match
// function.  This sample uses the 'soloduel' matching function to create 1v1
// matches.
//
// A typical approach if you wish to write your mmf in golang would be to make
// a copy of the open-match.dev/functions/golang/soloduel directory, write your
// own matchmaking logic in the 'Run' function based on your game's
// requirements, rename it according to what it does, and then compile this
// main program using your function in place of soloduel.
//
// A typical production deployment would put that compiled binary into a
// continer image to serve from a serverless platform like Cloud Run or
// kNative, or a kubernetes deployment with a service in front of it.
package main

import (
	"os"
	"strconv"

	mmf "open-match.dev/functions/golang/soloduel"
	"open-match.dev/mmf/server"
)

func main() {
	var port int

	// Check the knative/Cloud Run auto-populated env var for our port binding
	// Default to 8081
	{
		var ok bool
		var runPort string
		var err error

		if runPort, ok = os.LookupEnv("PORT"); ok {
			port, err = strconv.Atoi(runPort)
		}
		if err != nil || !ok {
			port = 8081
		}
	}

	mmfFifo := &mmf.MmfServer{}
	server.StartServer(int32(port), mmfFifo)
}
