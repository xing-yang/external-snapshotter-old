/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package connection

import (
	"github.com/container-storage-interface/spec/lib/go/csi/v0"
	crdv1 "github.com/kubernetes-csi/external-snapshotter/pkg/apis/volumesnapshot/v1alpha1"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ConvertSnapshotStatus converts snapshot status to crdv1.VolumeSnapshotCondition
func ConvertSnapshotStatus(status *csi.SnapshotStatus) crdv1.VolumeSnapshotCondition {
	var snapDataCondition crdv1.VolumeSnapshotCondition

	switch status.Type {
	case csi.SnapshotStatus_READY:
		snapDataCondition = crdv1.VolumeSnapshotCondition{
			Type:               crdv1.VolumeSnapshotConditionReady,
			Status:             v1.ConditionTrue,
			Message:            status.Details,
			LastTransitionTime: metav1.Now(),
		}
	case csi.SnapshotStatus_ERROR_UPLOADING:
		snapDataCondition = crdv1.VolumeSnapshotCondition{
			Type:               crdv1.VolumeSnapshotConditionError,
			Status:             v1.ConditionTrue,
			Message:            status.Details,
			LastTransitionTime: metav1.Now(),
		}
	case csi.SnapshotStatus_UPLOADING:
		snapDataCondition = crdv1.VolumeSnapshotCondition{
			Type:               crdv1.VolumeSnapshotConditionUploading,
			Status:             v1.ConditionTrue,
			Message:            status.Details,
			LastTransitionTime: metav1.Now(),
		}
	case csi.SnapshotStatus_UNKNOWN:
		snapDataCondition = crdv1.VolumeSnapshotCondition{
			Type:               crdv1.VolumeSnapshotConditionCreating,
			Status:             v1.ConditionUnknown,
			Message:            status.Details,
			LastTransitionTime: metav1.Now(),
		}
	}

	return snapDataCondition
}

// getSnapshotDataNameForSnapshot returns SnapshotData.Name for the create VolumeSnapshotData.
// The name must be unique.
func GetSnapshotDataNameForSnapshot(snapshot *crdv1.VolumeSnapshot) string {
	return "snapdata-" + string(snapshot.UID)
}
