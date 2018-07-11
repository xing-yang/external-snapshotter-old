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

package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/kubernetes-csi/external-snapshotter/pkg/connection"
	"k8s.io/api/core/v1"
	crdv1 "github.com/kubernetes-csi/external-snapshotter/pkg/apis/volumesnapshot/v1alpha1"
)

// Handler is responsible for handling VolumeAttachment events from informer.
type Handler interface {
	takeSnapshot(snapshot *crdv1.VolumeSnapshot, volume *v1.PersistentVolume, parameters map[string]string) (*crdv1.VolumeSnapshotData, *crdv1.VolumeSnapshotStatus, error)
	deleteSnapshot(vsd *crdv1.VolumeSnapshotData) error
	listSnapshots(vsd *crdv1.VolumeSnapshotData) (*crdv1.VolumeSnapshotCondition, error)
}

// csiHandler is a handler that calls CSI to create/delete volume snapshot.
type csiHandler struct {
	csiConnection connection.CSIConnection
	timeout       time.Duration
}

func NewCSIHandler(csiConnection connection.CSIConnection, timeout time.Duration) Handler {
	return &csiHandler{
		csiConnection: csiConnection,
		timeout:       timeout,
	}
}

func (handler *csiHandler) takeSnapshot(snapshot *crdv1.VolumeSnapshot,
	volume *v1.PersistentVolume, parameters map[string]string) (*crdv1.VolumeSnapshotData, *crdv1.VolumeSnapshotStatus, error) {
        glog.V(5).Infof("takeSnapshot: [%s]", snapshot.Name)
	ctx, cancel := context.WithTimeout(context.Background(), handler.timeout)
	defer cancel()

	snapDataObj, status, err := handler.csiConnection.CreateSnapshot(ctx, snapshot, volume, parameters)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to take snapshot of the volume %s: %q", volume.Name, err)
	}

	glog.V(5).Infof("takeSnapshot: Created snapshot [%s]. Snapshot object [%#v] Status [%#v]", snapshot.Name, snapDataObj, status)
	return snapDataObj, status, nil
}

func (handler *csiHandler) deleteSnapshot(vsd *crdv1.VolumeSnapshotData) error {
	if vsd.Spec.CSI == nil {
		return fmt.Errorf("CSISnapshot not defined in spec")
	}
	ctx, cancel := context.WithTimeout(context.Background(), handler.timeout)
	defer cancel()

	err := handler.csiConnection.DeleteSnapshot(ctx, vsd.Spec.CSI.SnapshotHandle)
	if err != nil {
		return fmt.Errorf("failed to delete snapshot data %s: %q", vsd.Name, err)
	}

	return nil
}

func (handler *csiHandler) listSnapshots(vsd *crdv1.VolumeSnapshotData) (*crdv1.VolumeSnapshotCondition, error) {
	if vsd.Spec.CSI == nil {
		return nil, fmt.Errorf("CSISnapshot not defined in spec")
	}
	ctx, cancel := context.WithTimeout(context.Background(), handler.timeout)
	defer cancel()

	snapshotDataCon, err := handler.csiConnection.ListSnapshots(ctx, vsd.Spec.CSI.SnapshotHandle)
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshot data %s: %q", vsd.Name, err)
	}

	return snapshotDataCon, nil
}
