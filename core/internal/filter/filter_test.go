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

package filter

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"open-match.dev/core/internal/filter/testcases"
	pb "open-match.dev/pkg/pb/v2"
)

// TestMeetsCriteria validates that tickets are correctly included/excluded
// from a pool based on that pool's filters (i.e. it validates the filter
// works as expected)
func TestMeetsCriteria(t *testing.T) {
	// Check that when a ticket meets filter criteria, it is included.
	testInclusion := func(t *testing.T, pool *pb.Pool, ticket *pb.Ticket) {
		if In(pool, ticket) {
			t.Error("ticket should be included in the pool")
		}
	}

	for _, tc := range testcases.IncludedTestCases() {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			testInclusion(t, tc.Pool, tc.Ticket)
		})
	}

	// Check that when a ticket fails filter criteria, it is excluded.
	testExclusion := func(t *testing.T, pool *pb.Pool, ticket *pb.Ticket) {
		if In(pool, ticket) {
			t.Error("ticket should be excluded from the pool")
		}
	}

	for _, tc := range testcases.ExcludedTestCases() {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			testExclusion(t, tc.Pool, tc.Ticket)
		})
	}
}

// TestValidPoolFilter runs the pool filter validation function with known
// invalid values to make sure the validation function correctly rejects them.
func TestValidPoolFilter(t *testing.T) {
	for _, tc := range []struct {
		name string
		pool *pb.Pool
		code codes.Code
		msg  string
	}{
		{
			"invalid creation time filter start value",
			&pb.Pool{
				CreationTimeRangeFilter: &pb.Pool_CreationTimeRangeFilter{
					Start: &timestamppb.Timestamp{Nanos: -1},
				},
			},
			codes.InvalidArgument,
			".invalid creation time filter start value",
		},
		{
			"invalid creation time filter end value",
			&pb.Pool{
				CreationTimeRangeFilter: &pb.Pool_CreationTimeRangeFilter{
					End: &timestamppb.Timestamp{Nanos: -1},
				},
			},
			codes.InvalidArgument,
			".invalid creation time filter end value",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ok, err := ValidatePoolFilters(tc.pool)

			require.False(t, ok)
			require.Error(t, err)

			s := status.Convert(err)
			require.Equal(t, tc.code, s.Code())
			require.Equal(t, tc.msg, s.Message())
		})
	}
}
