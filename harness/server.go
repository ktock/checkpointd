// Copyright 2026 checkpointd authors
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

package harness

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/ktock/checkpointd/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// KeepaliveEnforcementPolicy permits pings at least as frequent as
// checkpointd's own client-side workerKeepaliveParams
// (internal/harness/substrate/substrate.go).
var KeepaliveEnforcementPolicy = keepalive.EnforcementPolicy{
	MinTime:             20 * time.Second,
	PermitWithoutStream: true,
}

// Serve starts h's HarnessService at addr.
func Serve(addr string, h proto.HarnessServiceServer) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}
	srv := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(KeepaliveEnforcementPolicy))
	proto.RegisterHarnessServiceServer(srv, h)
	log.Printf("HarnessService listening on %s", addr)
	return srv.Serve(lis)
}
