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
	crdv1 "github.com/kubernetes-csi/external-snapshotter/pkg/apis/volumesnapshot/v1alpha1"
	clientset "github.com/kubernetes-csi/external-snapshotter/pkg/client/clientset/versioned"
	"github.com/kubernetes-csi/external-snapshotter/pkg/connection"
	"k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
)

// Handler is responsible for handling VolumeSnapshot events from informer.
type Handler interface {
	CreateSnapshotOperation(snapshot *crdv1.VolumeSnapshot) (*crdv1.VolumeSnapshot, error)
	DeleteSnapshotDataOperation(vsd *crdv1.VolumeSnapshotData) error
	ListSnapshots(vsd *crdv1.VolumeSnapshotData) (*crdv1.VolumeSnapshotCondition, error)
	BindandUpdateVolumeSnapshot(snapshotData *crdv1.VolumeSnapshotData, snapshot *crdv1.VolumeSnapshot, status *crdv1.VolumeSnapshotStatus) (*crdv1.VolumeSnapshot, error)
	GetClassFromVolumeSnapshot(snapshot *crdv1.VolumeSnapshot) (*crdv1.SnapshotClass, error)
	UpdateVolumeSnapshotStatus(snapshot *crdv1.VolumeSnapshot, condition *crdv1.VolumeSnapshotCondition) (*crdv1.VolumeSnapshot, error)
	GetSimplifiedSnapshotStatus(conditions []crdv1.VolumeSnapshotCondition) string
}

// csiHandler is a handler that calls CSI to create/delete volume snapshot.
type csiHandler struct {
	clientset                    clientset.Interface
	client                       kubernetes.Interface
	snapshotterName              string
	eventRecorder                record.EventRecorder
	csiConnection                connection.CSIConnection
	timeout                      time.Duration
	createSnapshotDataRetryCount int
	createSnapshotDataInterval   time.Duration
}

func NewCSIHandler(
	clientset clientset.Interface,
	client kubernetes.Interface,
	snapshotterName string,
	eventRecorder record.EventRecorder,
	csiConnection connection.CSIConnection,
	timeout time.Duration,
	createSnapshotDataRetryCount int,
	createSnapshotDataInterval time.Duration,
) Handler {
	return &csiHandler{
		clientset:       clientset,
		client:          client,
		snapshotterName: snapshotterName,
		eventRecorder:   eventRecorder,
		csiConnection:   csiConnection,
		timeout:         timeout,
		createSnapshotDataRetryCount: createSnapshotDataRetryCount,
		createSnapshotDataInterval:   createSnapshotDataInterval,
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

func (handler *csiHandler) ListSnapshots(vsd *crdv1.VolumeSnapshotData) (*crdv1.VolumeSnapshotCondition, error) {
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

// The function goes through the whole snapshot creation process.
// 1. Update VolumeSnapshot metadata to include the snapshotted PV name, timestamp and snapshot uid, also generate tag for cloud provider
// 2. Trigger the snapshot through cloud provider and attach the tag to the snapshot.
// 3. Create the VolumeSnapshotData object with the snapshot id information returned from step 2.
// 4. Bind the VolumeSnapshot and VolumeSnapshotData object
// 5. Query the snapshot status through cloud provider and update the status until snapshot is ready or fails.
func (handler *csiHandler) CreateSnapshotOperation(snapshot *crdv1.VolumeSnapshot) (*crdv1.VolumeSnapshot, error) {
	glog.Infof("createSnapshot: Creating snapshot %s through the plugin ...", vsToVsKey(snapshot))
	var result *crdv1.VolumeSnapshot
	var err error
	class, err := handler.GetClassFromVolumeSnapshot(snapshot)
	if err != nil {
		glog.Errorf("creatSnapshotOperation failed to getClassFromVolumeSnapshot %s", err)
		return nil, err
	}

	//  A previous createSnapshot may just have finished while we were waiting for
	//  the locks. Check that snapshot data (with deterministic name) hasn't been created
	//  yet.
	snapDataName := connection.GetSnapshotDataNameForSnapshot(snapshot)

	vsd, err := handler.clientset.VolumesnapshotV1alpha1().VolumeSnapshotDatas().Get(snapDataName, metav1.GetOptions{})
	if err == nil && vsd != nil {
		// Volume snapshot data has been already created, nothing to do.
		glog.V(4).Infof("createSnapshot [%s]: volume snapshot data already exists, skipping", vsToVsKey(snapshot))
		return nil, nil
	}
	glog.V(5).Infof("createSnapshotOperation [%s]: VolumeSnapshotData does not exist  yet", vsToVsKey(snapshot))

	volume, err := handler.getVolumeFromVolumeSnapshot(snapshot)
	if err != nil {
		glog.Errorf("createSnapshotOperation failed [%s]: Error: [%#v]", snapshot.Name, err)
		return nil, err
	}
	snapshotData, status, err := handler.takeSnapshot(snapshot, volume, class.Parameters)
	if err != nil {
		return nil, fmt.Errorf("failed to take snapshot of the volume %s: %q", volume.Name, err)
	}

	// Try to create the VSD object several times
	for i := 0; i < handler.createSnapshotDataRetryCount; i++ {
		glog.V(4).Infof("createSnapshot [%s]: trying to save volume snapshot data %s", vsToVsKey(snapshot), snapshotData.Name)
		if _, err = handler.clientset.VolumesnapshotV1alpha1().VolumeSnapshotDatas().Create(snapshotData); err == nil || apierrs.IsAlreadyExists(err) {
			// Save succeeded.
			if err != nil {
				glog.V(3).Infof("volume snapshot data %q for snapshot %q already exists, reusing", snapshotData.Name, vsToVsKey(snapshot))
				err = nil
			} else {
				glog.V(3).Infof("volume snapshot data %q for snapshot %q saved", snapshotData.Name, vsToVsKey(snapshot))
			}
			break
		}
		// Save failed, try again after a while.
		glog.V(3).Infof("failed to save volume snapshot data %q for snapshot %q: %v", snapshotData.Name, vsToVsKey(snapshot), err)
		time.Sleep(handler.createSnapshotDataInterval)
	}

	if err != nil {
		// Save failed. Now we have a storage asset outside of Kubernetes,
		// but we don't have appropriate volumesnapshotdata object for it.
		// Emit some event here and try to delete the storage asset several
		// times.
		strerr := fmt.Sprintf("Error creating volume snapshot data object for snapshot %s: %v. Deleting the snapshot data.", vsToVsKey(snapshot), err)
		glog.Error(strerr)
		handler.eventRecorder.Event(snapshot, v1.EventTypeWarning, "CreateSnapshotDataFailed", strerr)

		for i := 0; i < handler.createSnapshotDataRetryCount; i++ {
			if err = handler.deleteSnapshot(snapshotData); err == nil {
				// Delete succeeded
				glog.V(4).Infof("createSnapshot [%s]: cleaning snapshot data %s succeeded", vsToVsKey(snapshot), snapshotData.Name)
				break
			}
			// Delete failed, try again after a while.
			glog.Infof("failed to delete snapshot data %q: %v", snapshotData.Name, err)
			time.Sleep(handler.createSnapshotDataInterval)
		}

		if err != nil {
			// Delete failed several times. There is an orphaned volume snapshot data and there
			// is nothing we can do about it.
			strerr := fmt.Sprintf("Error cleaning volume snapshot data for snapshot %s: %v. Please delete manually.", vsToVsKey(snapshot), err)
			glog.Error(strerr)
			handler.eventRecorder.Event(snapshot, v1.EventTypeWarning, "SnapshotDataCleanupFailed", strerr)
		}
	} else {
		// save succeeded, bind and update status for snapshot.
		result, err = handler.BindandUpdateVolumeSnapshot(snapshotData, snapshot, status)
		if err != nil {
			return nil, err
		}
	}

	return result, err
}

// Delete a snapshot
// 1. Find the SnapshotData corresponding to Snapshot
//   1a: Not found => finish (it's been deleted already)
// 2. Ask the backend to remove the snapshot device
// 3. Delete the SnapshotData object
// 4. Remove the Snapshot from vsStore
// 5. Finish
func (handler *csiHandler) DeleteSnapshotDataOperation(vsd *crdv1.VolumeSnapshotData) error {
	glog.V(4).Infof("deleteSnapshotOperation [%s] started", vsd.Name)

	err := handler.deleteSnapshot(vsd)
	if err != nil {
		return fmt.Errorf("failed to delete snapshot %#v, err: %v", vsd.Name, err)
	}

	err = handler.clientset.VolumesnapshotV1alpha1().VolumeSnapshotDatas().Delete(vsd.Name, &metav1.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("failed to delete VolumeSnapshotData %s from API server: %q", vsd.Name, err)
	}

	return nil
}

func (handler *csiHandler) BindandUpdateVolumeSnapshot(snapshotData *crdv1.VolumeSnapshotData, snapshot *crdv1.VolumeSnapshot, status *crdv1.VolumeSnapshotStatus) (*crdv1.VolumeSnapshot, error) {
	glog.V(4).Infof("bindandUpdateVolumeSnapshot for snapshot [%s]: snapshotData [%s] status [%#v]", snapshot.Name, snapshotData.Name, status)
	snapshotObj, err := handler.clientset.VolumesnapshotV1alpha1().VolumeSnapshots(snapshot.Namespace).Get(snapshot.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("error get snapshot %s from api server: %v", vsToVsKey(snapshot), err)
	}

	// Copy the snapshot object before updating it
	snapshotCopy := snapshotObj.DeepCopy()
	var updateSnapshot *crdv1.VolumeSnapshot
	if snapshotObj.Spec.SnapshotDataName == snapshotData.Name {
		glog.Infof("bindVolumeSnapshotDataToVolumeSnapshot: VolumeSnapshot %s already bind to volumeSnapshotData [%s]", snapshot.Name, snapshotData.Name)
	} else {
		glog.Infof("bindVolumeSnapshotDataToVolumeSnapshot: before bind VolumeSnapshot %s to volumeSnapshotData [%s]", snapshot.Name, snapshotData.Name)
		snapshotCopy.Spec.SnapshotDataName = snapshotData.Name
		updateSnapshot, err = handler.clientset.VolumesnapshotV1alpha1().VolumeSnapshots(snapshot.Namespace).Update(snapshotCopy)
		if err != nil {
			glog.Infof("bindVolumeSnapshotDataToVolumeSnapshot: Error binding VolumeSnapshot %s to volumeSnapshotData [%s]. Error [%#v]", snapshot.Name, snapshotData.Name, err)
			return nil, fmt.Errorf("error updating snapshot object %s on the API server: %v", vsToVsKey(updateSnapshot), err)
		}
		snapshotCopy = updateSnapshot
	}

	if status != nil && status.Conditions != nil && len(status.Conditions) > 0 {
		snapshotCopy.Status = *(status.DeepCopy())
		updateSnapshot2, err := handler.clientset.VolumesnapshotV1alpha1().VolumeSnapshots(snapshot.Namespace).Update(snapshotCopy)
		if err != nil {
			return nil, fmt.Errorf("error updating snapshot object %s on the API server: %v", vsToVsKey(snapshotCopy), err)
		}
		snapshotCopy = updateSnapshot2
	}

	glog.V(5).Infof("bindandUpdateVolumeSnapshot for snapshot completed [%#v]", snapshotCopy)
	return snapshotCopy, nil
}

// UpdateVolumeSnapshotStatus update VolumeSnapshot status if the condition is changed.
func (handler *csiHandler) UpdateVolumeSnapshotStatus(snapshot *crdv1.VolumeSnapshot, condition *crdv1.VolumeSnapshotCondition) (*crdv1.VolumeSnapshot, error) {
	snapshotObj, err := handler.clientset.VolumesnapshotV1alpha1().VolumeSnapshots(snapshot.Namespace).Get(snapshot.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("error get volume snapshot %s from  api server: %s", vsToVsKey(snapshot), err)
	}

	oldStatus := snapshotObj.Status.DeepCopy()
	status := snapshotObj.Status
	isEqual := false
	if oldStatus.Conditions == nil ||
		len(oldStatus.Conditions) == 0 ||
		condition.Type != oldStatus.Conditions[len(oldStatus.Conditions)-1].Type {
		status.Conditions = append(status.Conditions, *condition)
	} else {
		oldCondition := oldStatus.Conditions[len(oldStatus.Conditions)-1]
		if condition.Status == oldCondition.Status {
			condition.LastTransitionTime = oldCondition.LastTransitionTime
		}
		status.Conditions[len(status.Conditions)-1] = *condition
		isEqual = condition.Type == oldCondition.Type &&
			condition.Status == oldCondition.Status &&
			condition.Reason == oldCondition.Reason &&
			condition.Message == oldCondition.Message &&
			condition.LastTransitionTime.Equal(&oldCondition.LastTransitionTime)
	}

	if !isEqual {
		snapshotObj.Status = status
		newSnapshotObj, err := handler.clientset.VolumesnapshotV1alpha1().VolumeSnapshots(snapshot.Namespace).Update(snapshotObj)
		if err != nil {
			return nil, fmt.Errorf("error update status for volume snapshot %s: %s", vsToVsKey(snapshot), err)
		}

		glog.Infof("UpdateVolumeSnapshotStatus finishes %+v", newSnapshotObj)
		return newSnapshotObj, nil
	}

	return nil, nil
}

// getSimplifiedSnapshotStatus get status for snapshot.
func (handler *csiHandler) GetSimplifiedSnapshotStatus(conditions []crdv1.VolumeSnapshotCondition) string {
	if conditions == nil {
		glog.Errorf("No conditions for this snapshot yet.")
		return statusNew
	}
	if len(conditions) == 0 {
		glog.Errorf("Empty condition.")
		return statusNew
	}

	//index := len(conditions) - 1
	lastCondition := conditions[len(conditions)-1]
	switch lastCondition.Type {
	case crdv1.VolumeSnapshotConditionReady:
		if lastCondition.Status == v1.ConditionTrue {
			return statusReady
		}
	case crdv1.VolumeSnapshotConditionError:
		return statusError
	case crdv1.VolumeSnapshotConditionUploading:
		if lastCondition.Status == v1.ConditionTrue ||
			lastCondition.Status == v1.ConditionUnknown {
			return statusUploading
		}
	}
	return statusNew
}

// getVolumeFromVolumeSnapshot is a helper function to get PV from VolumeSnapshot.
func (handler *csiHandler) getVolumeFromVolumeSnapshot(snapshot *crdv1.VolumeSnapshot) (*v1.PersistentVolume, error) {
	pvc, err := handler.getClaimFromVolumeSnapshot(snapshot)
	if err != nil {
		return nil, err
	}

	pvName := pvc.Spec.VolumeName
	pv, err := handler.client.CoreV1().PersistentVolumes().Get(pvName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve PV %s from the API server: %q", pvName, err)
	}

	glog.V(5).Infof("getVolumeFromVolumeSnapshot: snapshot [%s] PV name [%s]", snapshot.Name, pvName)

	return pv, nil
}

// getClassFromVolumeSnapshot is a helper function to get storage class from VolumeSnapshot.
func (handler *csiHandler) GetClassFromVolumeSnapshot(snapshot *crdv1.VolumeSnapshot) (*crdv1.SnapshotClass, error) {
	className := snapshot.Spec.SnapshotClassName
	glog.V(5).Infof("getClassFromVolumeSnapshot [%s]: SnapshotClassName [%s]", snapshot.Name, className)
	class, err := handler.clientset.VolumesnapshotV1alpha1().SnapshotClasses().Get(className, metav1.GetOptions{})
	if err != nil {
		glog.Errorf("failed to retrieve storage class %s from the API server: %q", className, err)
		//return nil, fmt.Errorf("failed to retrieve storage class %s from the API server: %q", className, err)
	}
	return class, nil
}

// getClaimFromVolumeSnapshot is a helper function to get PV from VolumeSnapshot.
func (handler *csiHandler) getClaimFromVolumeSnapshot(snapshot *crdv1.VolumeSnapshot) (*v1.PersistentVolumeClaim, error) {
	pvcName := snapshot.Spec.PersistentVolumeClaimName
	if pvcName == "" {
		return nil, fmt.Errorf("the PVC name is not specified in snapshot %s", vsToVsKey(snapshot))
	}

	pvc, err := handler.client.CoreV1().PersistentVolumeClaims(snapshot.Namespace).Get(pvcName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve PVC %s from the API server: %q", pvcName, err)
	}
	if pvc.Status.Phase != v1.ClaimBound {
		return nil, fmt.Errorf("the PVC %s not yet bound to a PV, will not attempt to take a snapshot yet", pvcName)
	}

	return pvc, nil
}

// getSnapshotDataFromSnapshot looks up VolumeSnapshotData from a VolumeSnapshot.
func (handler *csiHandler) getSnapshotDataFromSnapshot(vs *crdv1.VolumeSnapshot) (*crdv1.VolumeSnapshotData, error) {
	snapshotDataName := vs.Spec.SnapshotDataName
	if snapshotDataName == "" {
		return nil, fmt.Errorf("could not find snapshot data object for %s: SnapshotDataName in snapshot spec is empty", vsToVsKey(vs))
	}

	snapshotDataObj, err := handler.clientset.VolumesnapshotV1alpha1().VolumeSnapshotDatas().Get(snapshotDataName, metav1.GetOptions{})
	if err != nil {
		glog.Errorf("Error retrieving the VolumeSnapshotData objects from API server: %v", err)
		return nil, fmt.Errorf("could not get snapshot data object %s: %v", snapshotDataName, err)
	}

	return snapshotDataObj, nil
}
