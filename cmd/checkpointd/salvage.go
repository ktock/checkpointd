// Copyright 2026 Google LLC
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

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ktock/checkpointd/internal/controller"
	"github.com/ktock/checkpointd/internal/controller/eventlog"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// podExistenceChecker abstracts the real Kubernetes Pod check the salvage
// sweep needs, so its own logic can be unit tested against a fake, with no
// real API server needed.
type podExistenceChecker interface {
	// PodExists reports whether a pod named podName, with exactly uid,
	// currently exists and is not Failed. Any other error (timeout,
	// forbidden, etc.) is returned as a non-nil err and MUST NOT be
	// treated as "not alive" by the caller.
	PodExists(ctx context.Context, namespace, podName, uid string) (bool, error)
}

// clientsetPodExistenceChecker is podExistenceChecker's real,
// client-go-backed implementation.
type clientsetPodExistenceChecker struct {
	cs kubernetes.Interface
}

// newPodExistenceChecker authenticates as checkpointd-server's own
// ServiceAccount.
func newPodExistenceChecker() (podExistenceChecker, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("loading in-cluster Kubernetes config (salvage requires a real cluster): %w", err)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("building Kubernetes clientset: %w", err)
	}
	return &clientsetPodExistenceChecker{cs: cs}, nil
}

func (c *clientsetPodExistenceChecker) PodExists(ctx context.Context, namespace, podName, uid string) (bool, error) {
	pod, err := c.cs.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("getting pod %s/%s: %w", namespace, podName, err)
	}
	if string(pod.UID) != uid {
		return false, nil
	}
	return pod.Status.Phase != corev1.PodFailed, nil
}

// sweepOrphanedSessions finds every session whose recorded (owner_pod,
// owner_uid) no longer matches a currently live pod, and claims each for
// (thisPod, thisUID), resuming its relay loop locally.
func sweepOrphanedSessions(ctx context.Context, c *controller.Controller, el eventlog.EventLog, store *sqlTaskStore, registry *taskRegistry, checker podExistenceChecker, namespace, thisPod, thisUID string) (claimed int, err error) {
	owners, err := store.ListDistinctActiveOwners(ctx)
	if err != nil {
		return 0, fmt.Errorf("salvage: listing distinct active owners: %w", err)
	}
	for _, owner := range owners {
		if owner.pod == thisPod && owner.uid == thisUID {
			continue // We're obviously alive.
		}
		alive, err := checker.PodExists(ctx, namespace, owner.pod, owner.uid)
		if err != nil {
			// Fail closed: never treat an ambiguous/transient API error as
			// "not alive". Skip this owner, retry next sweep.
			log.Infof("salvage: checking pod %s/%s (uid %s): %v", namespace, owner.pod, owner.uid, err)
			continue
		}
		if alive {
			continue
		}
		n, err := salvageSessionsOwnedBy(ctx, c, el, store, registry, owner.pod, owner.uid, thisPod, thisUID)
		if err != nil {
			return claimed, err
		}
		claimed += n
	}
	return claimed, nil
}

// salvageSessionsOwnedBy claims and resumes every non-terminal session
// currently recorded as owned by exactly (podName, ownerUID) -- the caller
// has already confirmed this specific incarnation is not currently alive.
func salvageSessionsOwnedBy(ctx context.Context, c *controller.Controller, el eventlog.EventLog, store *sqlTaskStore, registry *taskRegistry, podName, ownerUID, thisPod, thisUID string) (claimed int, err error) {
	pageToken := ""
	for {
		sessions, next, err := store.ListSessionsOwnedByPodUID(ctx, pageToken, podName, ownerUID)
		if err != nil {
			return claimed, fmt.Errorf("salvage: listing sessions owned by %s (uid %s): %w", podName, ownerUID, err)
		}
		for _, sess := range sessions {
			ok, err := store.ClaimOrphanedSession(ctx, sess.id, thisPod, thisUID, ownerUID)
			if err != nil {
				log.Infof("salvage: claiming orphaned session %s: %v", sess.id, err)
				continue
			}
			if !ok {
				// Another instance's sweep (or that pod's own self-recovery,
				// if it came back before we got here) already claimed it.
				continue
			}
			log.Infof("salvage: session %s orphaned (previous owner %s/%s no longer live), claimed by %s/%s", sess.id, podName, ownerUID, thisPod, thisUID)
			if err := resumeClaimedSession(ctx, c, el, store, registry, sess.id, sess.state); err != nil {
				log.Infof("salvage: resuming claimed session %s: %v", sess.id, err)
				mustRevertClaim(ctx, store, sess.id, podName, ownerUID, thisUID)
				continue
			}
			claimed++
		}
		if next == "" {
			break
		}
		pageToken = next
	}
	return claimed, nil
}

func runSalvageLoop(ctx context.Context, c *controller.Controller, el eventlog.EventLog, store *sqlTaskStore, registry *taskRegistry, checker podExistenceChecker, namespace, thisPod, thisUID string, interval time.Duration) {
	sweepOnce := func() {
		n, err := sweepOrphanedSessions(ctx, c, el, store, registry, checker, namespace, thisPod, thisUID)
		if err != nil {
			log.Infof("salvage: sweep failed: %v", err)
			return
		}
		if n > 0 {
			log.Infof("salvage: claimed and resumed %d orphaned session(s)", n)
		}
	}
	sweepOnce()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweepOnce()
		}
	}
}
