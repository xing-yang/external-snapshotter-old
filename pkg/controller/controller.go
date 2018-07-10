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
	"fmt"
	"time"

	"github.com/golang/glog"
	snapshotv1alpha1 "github.com/kubernetes-csi/external-snapshotter/pkg/apis/volumesnapshot/v1alpha1"
	snapshotclientset "github.com/kubernetes-csi/external-snapshotter/pkg/client/clientset/versioned"
	snapshotinformers "github.com/kubernetes-csi/external-snapshotter/pkg/client/informers/externalversions/volumesnapshot/v1alpha1"
	snapshotlisters "github.com/kubernetes-csi/external-snapshotter/pkg/client/listers/volumesnapshot/v1alpha1"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	ref "k8s.io/client-go/tools/reference"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/util/goroutinemap"
	"k8s.io/kubernetes/pkg/util/goroutinemap/exponentialbackoff"
)

// defaultSnapshotSyncPeriod is the default period for syncing volume snapshots
// and volume snapshot datas.
const defaultSnapshotSyncPeriod = 20 * time.Second

// annBindCompleted annotation applies to snapshots. It indicates that the lifecycle
// of the snapshot has passed through the initial setup. This information changes how
// we interpret some observations of the state of the objects. Value of this
// annotation does not matter.
const annBindCompleted = "snapshot.kubernetes.io/bind-completed"

// This annotation is added to a snapshotData that has been dynamically created by
// Kubernetes. Its value is name of volume plugin that created the snapshot data.
// It serves both user (to show where a snapshotData comes from) and Kubernetes (to
// recognize dynamically created snapshotDatas in its decisions).
const annDynamicallyCreatedBy = "snapshotData.kubernetes.io/created-by"

// annBoundByController annotation applies to VSDs and Vss.  It indicates that
// the binding (VSD->VS or VS->VSD) was installed by the controller.  The
// absence of this annotation means the binding was done by the user (i.e.
// pre-bound). Value of this annotation does not matter.
const annBoundByController = "snapshotData.kubernetes.io/bound-by-controller"

// ControllerParameters contains arguments for creation of a new
// SnapshotController controller.
type ControllerParameters struct {
	Snapshotter                  string
	Handler                      Handler
	KubeClient                   clientset.Interface
	SnapshotClient               snapshotclientset.Interface
	VolumeInformer               coreinformers.PersistentVolumeInformer
	ClaimInformer                coreinformers.PersistentVolumeClaimInformer
	VolumeSnapshotInformer       snapshotinformers.VolumeSnapshotInformer
	VolumeSnapshotDataInformer   snapshotinformers.VolumeSnapshotDataInformer
	SnapshotClassInformer        snapshotinformers.SnapshotClassInformer
	EventRecorder                record.EventRecorder
	CreateSnapshotDataRetryCount int
	CreateSnapshotDataInterval   time.Duration
}

type CSISnapshotController struct {
	snapshotLister           snapshotlisters.VolumeSnapshotLister
	snapshotListerSynced     cache.InformerSynced
	snapshotDataLister       snapshotlisters.VolumeSnapshotDataLister
	snapshotDataListerSynced cache.InformerSynced
	snapshotClassLister      snapshotlisters.SnapshotClassLister
	snapshotClassSynced      cache.InformerSynced
	volumeLister             corelisters.PersistentVolumeLister
	volumeListerSynced       cache.InformerSynced
	claimLister              corelisters.PersistentVolumeClaimLister
	claimListerSynced        cache.InformerSynced

	client         kubernetes.Interface
	snapshotClient snapshotclientset.Interface
	eventRecorder  record.EventRecorder
	handler        Handler
	snapshotter    string
	resyncPeriod   time.Duration

	// Map of scheduled/running operations.
	runningOperations            goroutinemap.GoRoutineMap
	createSnapshotDataRetryCount int
	createSnapshotDataInterval   time.Duration

	snapshotQueue     workqueue.RateLimitingInterface
	snapshotDataQueue workqueue.RateLimitingInterface

	snapshotStore     cache.Store
	snapshotDataStore cache.Store
}

// NewCSISnapshotController creates a new VolumeSnapshot controller
func NewCSISnapshotController(p ControllerParameters) *CSISnapshotController {
	eventRecorder := p.EventRecorder
	if eventRecorder == nil {
		broadcaster := record.NewBroadcaster()
		broadcaster.StartLogging(glog.Infof)
		broadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: p.KubeClient.CoreV1().Events("")})
		eventRecorder = broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "CSISnapshotController"})
	}

	ctrl := &CSISnapshotController{
		snapshotter:                  p.Snapshotter,
		handler:                      p.Handler,
		client:                       p.KubeClient,
		snapshotClient:               p.SnapshotClient,
		eventRecorder:                eventRecorder,
		createSnapshotDataRetryCount: p.CreateSnapshotDataRetryCount,
		createSnapshotDataInterval:   p.CreateSnapshotDataInterval,
		runningOperations:            goroutinemap.NewGoRoutineMap(true),
		snapshotStore:                cache.NewStore(cache.DeletionHandlingMetaNamespaceKeyFunc),
		snapshotDataStore:            cache.NewStore(cache.DeletionHandlingMetaNamespaceKeyFunc),
		snapshotQueue:                workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "csi-snapshotter-vs"),
		snapshotDataQueue:            workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "csi-snapshotter-vsd"),
	}

	p.VolumeSnapshotInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj interface{}) { ctrl.enqueueWork(ctrl.snapshotQueue, obj) },
			UpdateFunc: func(oldObj, newObj interface{}) { ctrl.enqueueWork(ctrl.snapshotQueue, newObj) },
			DeleteFunc: func(obj interface{}) { ctrl.enqueueWork(ctrl.snapshotQueue, obj) },
		},
	)
	ctrl.snapshotLister = p.VolumeSnapshotInformer.Lister()
	ctrl.snapshotListerSynced = p.VolumeSnapshotInformer.Informer().HasSynced

	p.VolumeSnapshotDataInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj interface{}) { ctrl.enqueueWork(ctrl.snapshotDataQueue, obj) },
			UpdateFunc: func(oldObj, newObj interface{}) { ctrl.enqueueWork(ctrl.snapshotDataQueue, newObj) },
			DeleteFunc: func(obj interface{}) { ctrl.enqueueWork(ctrl.snapshotDataQueue, obj) },
		},
	)
	ctrl.snapshotDataLister = p.VolumeSnapshotDataInformer.Lister()
	ctrl.snapshotDataListerSynced = p.VolumeSnapshotDataInformer.Informer().HasSynced

	ctrl.volumeLister = p.VolumeInformer.Lister()
	ctrl.volumeListerSynced = p.VolumeInformer.Informer().HasSynced

	ctrl.claimLister = p.ClaimInformer.Lister()
	ctrl.claimListerSynced = p.ClaimInformer.Informer().HasSynced

	return ctrl
}

// enqueueWork adds snapshot or snapshotData to given work queue.
func (ctrl *CSISnapshotController) enqueueWork(queue workqueue.Interface, obj interface{}) {
	// Beware of "xxx deleted" events
	if unknown, ok := obj.(cache.DeletedFinalStateUnknown); ok && unknown.Obj != nil {
		obj = unknown.Obj
	}
	objName, err := keyFunc(obj)
	if err != nil {
		glog.Errorf("failed to get key from object: %v", err)
		return
	}
	glog.V(5).Infof("enqueued %q for sync", objName)
	queue.Add(objName)
}

func (ctrl *CSISnapshotController) Run(workers int, stopCh <-chan struct{}) {
	defer ctrl.snapshotQueue.ShutDown()
	defer ctrl.snapshotDataQueue.ShutDown()

	glog.Infof("Starting snapshot controller")
	defer glog.Infof("Shutting snapshot controller")

	if !cache.WaitForCacheSync(stopCh, ctrl.snapshotListerSynced, ctrl.snapshotDataListerSynced, ctrl.volumeListerSynced, ctrl.claimListerSynced) {
		glog.Errorf("Cannot sync caches")
		return
	}

	ctrl.initializeCaches(ctrl.snapshotLister, ctrl.snapshotDataLister)

	for i := 0; i < workers; i++ {
		go wait.Until(ctrl.snapshotWorker, 0, stopCh)
		go wait.Until(ctrl.snapshotDataWorker, 0, stopCh)
	}

	<-stopCh
}

// snapshotWorker processes items from snapshotQueue. It must run only once,
// syncVolume is not assured to be reentrant.
func (ctrl *CSISnapshotController) snapshotWorker() {
	workFunc := func() bool {
		keyObj, quit := ctrl.snapshotQueue.Get()
		if quit {
			return true
		}
		defer ctrl.snapshotQueue.Done(keyObj)
		key := keyObj.(string)
		glog.V(5).Infof("snapshotWorker[%s]", key)

		namespace, name, err := cache.SplitMetaNamespaceKey(key)
		if err != nil {
			glog.V(4).Infof("error getting namespace & name of snapshot %q to get snapshot from informer: %v", key, err)
			return false
		}
		snapshot, err := ctrl.snapshotLister.VolumeSnapshots(namespace).Get(name)
		if err == nil {
			// The volume snapshot still exists in informer cache, the event must have
			// been add/update/sync
			ctrl.updateSnapshot(snapshot)
			return false
		}
		if !errors.IsNotFound(err) {
			glog.V(2).Infof("error getting snapshot %q from informer: %v", key, err)
			return false
		}
		// The snapshot is not in informer cache, the event must have been "delete"
		vsObj, found, err := ctrl.snapshotStore.GetByKey(key)
		if err != nil {
			glog.V(2).Infof("error getting snapshot %q from cache: %v", key, err)
			return false
		}
		if !found {
			// The controller has already processed the delete event and
			// deleted the snapshot from its cache
			glog.V(2).Infof("deletion of vs %q was already processed", key)
			return false
		}
		snapshot, ok := vsObj.(*snapshotv1alpha1.VolumeSnapshot)
		if !ok {
			glog.Errorf("expected vs, got %+v", vsObj)
			return false
		}

		ctrl.deleteSnapshot(snapshot)

		return false
	}

	for {
		if quit := workFunc(); quit {
			glog.Infof("snapshot worker queue shutting down")
			return
		}
	}
}

// snapshotDataWorker processes items from snapshotDataQueue. It must run only once,
// syncSnapshotData is not assured to be reentrant.
func (ctrl *CSISnapshotController) snapshotDataWorker() {
	workFunc := func() bool {
		keyObj, quit := ctrl.snapshotDataQueue.Get()
		if quit {
			return true
		}
		defer ctrl.snapshotDataQueue.Done(keyObj)
		key := keyObj.(string)
		glog.V(5).Infof("snapshotDataWorker[%s]", key)

		_, name, err := cache.SplitMetaNamespaceKey(key)
		if err != nil {
			glog.V(4).Infof("error getting name of snapshotData %q to get snapshotData from informer: %v", key, err)
			return false
		}
		snapshotData, err := ctrl.snapshotDataLister.Get(name)
		if err == nil {
			// The snapshotData still exists in informer cache, the event must have
			// been add/update/sync
			ctrl.updateSnapshotData(snapshotData)
			return false
		}
		if !errors.IsNotFound(err) {
			glog.V(2).Infof("error getting snapshotData %q from informer: %v", key, err)
			return false
		}

		// The snapshotData is not in informer cache, the event must have been "delete"
		snapshotDataObj, found, err := ctrl.snapshotDataStore.GetByKey(key)
		if err != nil {
			glog.V(2).Infof("error getting snapshotData %q from cache: %v", key, err)
			return false
		}
		if !found {
			// The controller has already processed the delete event and
			// deleted the snapshotData from its cache
			glog.V(2).Infof("deletion of snapshotData %q was already processed", key)
			return false
		}
		snapshotData, ok := snapshotDataObj.(*snapshotv1alpha1.VolumeSnapshotData)
		if !ok {
			glog.Errorf("expected snapshotData, got %+v", snapshotData)
			return false
		}
		ctrl.deleteSnapshotData(snapshotData)
		return false
	}

	for {
		if quit := workFunc(); quit {
			glog.Infof("snapshotData worker queue shutting down")
			return
		}
	}
}

// initializeCaches fills all controller caches with initial data from etcd in
// order to have the caches already filled when first addSnapshot/addSnapshotData to
// perform initial synchronization of the controller.
func (ctrl *CSISnapshotController) initializeCaches(snapshotLister snapshotlisters.VolumeSnapshotLister, snapshotDataLister snapshotlisters.VolumeSnapshotDataLister) {
	snapshotList, err := snapshotLister.List(labels.Everything())
	if err != nil {
		glog.Errorf("CSISnapshotController can't initialize caches: %v", err)
		return
	}
	for _, snapshot := range snapshotList {
		snapshotClone := snapshot.DeepCopy()
		if _, err = ctrl.storeSnapshotUpdate(snapshotClone); err != nil {
			glog.Errorf("error updating volume snapshot cache: %v", err)
		}
	}

	snapshotDataList, err := snapshotDataLister.List(labels.Everything())
	if err != nil {
		glog.Errorf("CSISnapshotController can't initialize caches: %v", err)
		return
	}
	for _, snapshotData := range snapshotDataList {
		snapshotDataClone := snapshotData.DeepCopy()
		if _, err = ctrl.storeSnapshotDataUpdate(snapshotDataClone); err != nil {
			glog.Errorf("error updating volume snapshotData cache: %v", err)
		}
	}

	glog.V(4).Infof("controller initialized")
}

func (ctrl *CSISnapshotController) storeSnapshotUpdate(snapshot interface{}) (bool, error) {
	return storeObjectUpdate(ctrl.snapshotStore, snapshot, "snapshot")
}

func (ctrl *CSISnapshotController) storeSnapshotDataUpdate(snapshotData interface{}) (bool, error) {
	return storeObjectUpdate(ctrl.snapshotDataStore, snapshotData, "snapshotdata")
}

// deleteSnapshotData runs in worker thread and handles "snapshotData deleted" event.
func (ctrl *CSISnapshotController) deleteSnapshotData(snapshotData *snapshotv1alpha1.VolumeSnapshotData) {
	_ = ctrl.snapshotDataStore.Delete(snapshotData)
	glog.V(4).Infof("snapshotData %q deleted", snapshotData.Name)

	snapshotName := VsRefToVsKey(snapshotData.Spec.VolumeSnapshotRef)
	if snapshotName == "" {
		glog.V(5).Infof("deleteSnapshotData[%q]: snapshotData not bound", snapshotData.Name)
		return
	}
	// sync the snapshot when its snapshotData is deleted.  Explicitly sync'ing the
	// snapshot here in response to snapshotData deletion prevents the snapshot from
	// waiting until the next sync period for its Release.
	glog.V(5).Infof("deleteSnapshotData[%q]: scheduling sync of snapshot %s", snapshotData.Name, snapshotName)
	ctrl.snapshotDataQueue.Add(snapshotName)
}

// deleteSnapshot runs in worker thread and handles "snapshot deleted" event.
func (ctrl *CSISnapshotController) deleteSnapshot(snapshot *snapshotv1alpha1.VolumeSnapshot) {
	_ = ctrl.snapshotStore.Delete(snapshot)
	glog.V(4).Infof("snapshot %q deleted", VsToVsKey(snapshot))

	snapshotDataName := snapshot.Spec.SnapshotDataName
	if snapshotDataName == "" {
		glog.V(5).Infof("deleteSnapshot[%q]: snapshot not bound", VsToVsKey(snapshot))
		return
	}
	// sync the snapshotData when its snapshot is deleted.  Explicitly sync'ing the
	// snapshotData here in response to snapshot deletion prevents the snapshotData from
	// waiting until the next sync period for its Release.
	glog.V(5).Infof("deleteSnapshot[%q]: scheduling sync of snapshotData %s", VsToVsKey(snapshot), snapshotDataName)
	ctrl.snapshotDataQueue.Add(snapshotDataName)
}

// updateSnapshot runs in worker thread and handles "snapshot added",
// "snapshot updated" and "periodic sync" events.
func (ctrl *CSISnapshotController) updateSnapshot(snapshot *snapshotv1alpha1.VolumeSnapshot) {
	// Store the new snapshot version in the cache and do not process it if this is
	// an old version.
	new, err := ctrl.storeSnapshotUpdate(snapshot)
	if err != nil {
		glog.Errorf("%v", err)
	}
	if !new {
		return
	}
	err = ctrl.syncSnapshot(snapshot)
	if err != nil {
		if errors.IsConflict(err) {
			// Version conflict error happens quite often and the controller
			// recovers from it easily.
			glog.V(3).Infof("could not sync snapshot %q: %+v", VsToVsKey(snapshot), err)
		} else {
			glog.Errorf("could not sync snapshot %q: %+v", VsToVsKey(snapshot), err)
		}
	}
}

// updateSnapshotData runs in worker thread and handles "snapshotData added",
// "snapshotData updated" and "periodic sync" events.
func (ctrl *CSISnapshotController) updateSnapshotData(snapshotData *snapshotv1alpha1.VolumeSnapshotData) {
	// Store the new updateSnapshotData version in the cache and do not process it if this is
	// an old version.
	new, err := ctrl.storeSnapshotDataUpdate(snapshotData)
	if err != nil {
		glog.Errorf("%v", err)
	}
	if !new {
		return
	}
	err = ctrl.syncSnapshotData(snapshotData)
	if err != nil {
		if errors.IsConflict(err) {
			// Version conflict error happens quite often and the controller
			// recovers from it easily.
			glog.V(3).Infof("could not sync snapshotData %q: %+v", snapshotData.Name, err)
		} else {
			glog.Errorf("could not sync snapshotData %q: %+v", snapshotData.Name, err)
		}
	}
}

// syncSnapshotData deals with one key off the queue.  It returns false when it's time to quit.
func (ctrl *CSISnapshotController) syncSnapshotData(snapshotData *snapshotv1alpha1.VolumeSnapshotData) error {
	glog.V(4).Infof("synchronizing VolumeSnapshotData[%s]", snapshotData.Name)

	// VolumeSnapshotData is not bind to any VolumeSnapshot, this case rare and we just return err
	if snapshotData.Spec.VolumeSnapshotRef == nil {
		// VolumeSnapshotData is not bind
		glog.V(4).Infof("synchronizing VolumeSnapshotData[%s]: VolumeSnapshotData is not bind", snapshotData.Name)
		return fmt.Errorf("volumeSnapshotData %s is not bind to any VolumeSnapshot", snapshotData.Name)
	} else {
		glog.V(4).Infof("synchronizing VolumeSnapshotData[%s]: VolumeSnapshotData is bound to snapshot %s", snapshotData.Name, VsRefToVsKey(snapshotData.Spec.VolumeSnapshotRef))
		// Get the snapshot by _name_
		var snapshot *snapshotv1alpha1.VolumeSnapshot
		vsName := VsRefToVsKey(snapshotData.Spec.VolumeSnapshotRef)
		obj, found, err := ctrl.snapshotStore.GetByKey(vsName)
		if err != nil {
			return err
		}
		if !found {
			glog.V(4).Infof("synchronizing VolumeSnapshotData[%s]: snapshot %s not found", snapshotData.Name, VsRefToVsKey(snapshotData.Spec.VolumeSnapshotRef))
			// Fall through with snapshot = nil
		} else {
			var ok bool
			snapshot, ok = obj.(*snapshotv1alpha1.VolumeSnapshot)
			if !ok {
				return fmt.Errorf("cannot convert object from snapshot cache to snapshot %q!?: %#v", snapshotData.Name, obj)
			}
			glog.V(4).Infof("synchronizing VolumeSnapshotData[%s]: snapshot %s found", snapshotData.Name, VsRefToVsKey(snapshotData.Spec.VolumeSnapshotRef))
		}
		if snapshot != nil && snapshot.UID != snapshotData.Spec.VolumeSnapshotRef.UID {
			// The snapshot that the snapshotData was pointing to was deleted, and another
			// with the same name created.
			glog.V(4).Infof("synchronizing VolumeSnapshotData[%s]: snapshotData %s has different UID, the old one must have been deleted", snapshotData.Name, VsRefToVsKey(snapshotData.Spec.VolumeSnapshotRef))
			// Treat the snapshot data as bound to a missing snapshot.
			snapshot = nil
		}
		if snapshot == nil {
			// If we get into this block, the snapshot must have been deleted, so we just delete the snapshot data.
			glog.V(4).Infof("delete backend storage snapshot data [%s]", snapshotData.Name)
			opName := fmt.Sprintf("delete-%s[%s]", snapshotData.Name, string(snapshotData.UID))
			ctrl.scheduleOperation(opName, func() error {
				return ctrl.deleteSnapshotDataOperation(snapshotData)
			})
			return nil
		} else if snapshot.Spec.SnapshotDataName == "" {
			if metav1.HasAnnotation(snapshotData.ObjectMeta, annBoundByController) {
				// The binding is not completed; let snapshot sync handle it
				glog.V(4).Infof("synchronizing VolumeSnapshotData[%s]: snapshotData is not bound yet, waiting for syncSnapshot to fix it", snapshotData.Name)
			} else {
				// Dangling snapshotData; try to re-establish the link in the snapshot sync
				glog.V(4).Infof("synchronizing VolumeSnapshotData[%s]: snapshotData was bound and got unbound (by user?), waiting for syncSnapshot to fix it", snapshotData.Name)
			}
			// In both cases, the snapshotData is Bound and the snapshot is not bound to it.
			// Next syncSnapshot will fix it. To speed it up, we enqueue the snapshot
			// into the controller, which results in syncSnapshot to be called
			// shortly (and in the right worker goroutine).
			// This speeds up binding of created snapshotDatas - snapshotter saves
			// only the new VSD and it expects that next syncSnapshot will bind the
			// snapshot to it.
			ctrl.snapshotQueue.Add(VsToVsKey(snapshot))
			return nil
		} else if snapshot.Spec.SnapshotDataName == snapshotData.Name {
			// SnapshotData is bound to a snapshot properly, update status if necessary
			glog.V(4).Infof("synchronizing VolumeSnapshotData[%s]: all is bound", snapshotData.Name)

			// Query the driver for the status of the snapshotData with snapshot id
			// from VolumeSnapshotData object.
			snapshotCond, err := ctrl.handler.listSnapshots(snapshotData)
			if err != nil {
				// Nothing was saved; we will fall back into the same
				// condition in the next call to this method
				return fmt.Errorf("failed to check snapshotData status %s: %v", snapshotData.Name, err)
			}
			if _, err := ctrl.snapshotDataConditionUpdate(snapshotData, snapshotCond); err != nil {
				return err
			}
			return nil
		} else {
			// SnapshotData is bound to a snapshot, but the snapshot is bound elsewhere
			if metav1.HasAnnotation(snapshotData.ObjectMeta, annDynamicallyCreatedBy) {
				// This snapshotData was dynamically created for this snapshot. The
				// snapshot got bound elsewhere, and thus this snapshotData is not
				// needed. Delete it.
				glog.V(4).Infof("delete backend storage snapshot data [%s]", snapshotData.Name)
				opName := fmt.Sprintf("delete-%s[%s]", snapshotData.Name, string(snapshotData.UID))
				ctrl.scheduleOperation(opName, func() error {
					return ctrl.deleteSnapshotDataOperation(snapshotData)
				})
				return nil
			} else {
				// snapshotData is bound to a snapshot, but the snapshot is bound elsewhere
				// and it's not dynamically created.
				if metav1.HasAnnotation(snapshotData.ObjectMeta, annBoundByController) {
					// This is part of the normal operation of the controller; the
					// controller tried to use this snapshotData for a snapshot but the snapshot
					// was fulfilled by another snapshotData. We did this; fix it.
					glog.V(4).Infof("synchronizing VolumeSnapshotData[%s]: snapshotData is bound by controller to a snapshot that is bound to another snapshotData, unbinding", snapshotData.Name)
					if err = ctrl.unbindSnapshotData(snapshotData); err != nil {
						return err
					}
					return nil
				} else {
					// The snapshotData must have been created with this ptr; leave it alone.
					glog.V(4).Infof("synchronizing VolumeSnapshotData[%s]: snapshotData is bound by user to a snapshot that is bound to another snapshotData, waiting for the snapshot to get unbound", snapshotData.Name)
					// This just updates clears snapshotData.Spec.VolumeSnapshotRef.UID. It leaves the
					// snapshotData pre-bound to the snapshot.
					if err = ctrl.unbindSnapshotData(snapshotData); err != nil {
						return err
					}
					return nil
				}
			}
		}
	}
}

// syncSnapshot is the main controller method to decide what to do with a snapshot.
// It's invoked by appropriate cache.Controller callbacks when a snapshot is
// created, updated or periodically synced. We do not differentiate between
// these events.
// For easier readability, it was split into syncUnboundSnapshot and syncBoundSnapshot
// methods.
func (ctrl *CSISnapshotController) syncSnapshot(snapshot *snapshotv1alpha1.VolumeSnapshot) error {
	glog.V(4).Infof("synchronizing VolumeSnapshot[%s]: %s", VsToVsKey(snapshot))

	if !metav1.HasAnnotation(snapshot.ObjectMeta, annBindCompleted) {
		return ctrl.syncUnboundSnapshot(snapshot)
	} else {
		return ctrl.syncBoundSnapshot(snapshot)
	}
}

// syncUnboundSnapshot is the main controller method to decide what to do with an
// unbound snapshot.
func (ctrl *CSISnapshotController) syncUnboundSnapshot(snapshot *snapshotv1alpha1.VolumeSnapshot) error {
	// This is a new Snapshot that has not completed binding
	if snapshot.Spec.SnapshotDataName == "" {
		snapshotData := ctrl.findMatchSnapshotData(snapshot)
		if snapshotData == nil {
			glog.V(4).Infof("synchronizing unbound VolumeSnapshot[%s]: no SnapshotData found", VsToVsKey(snapshot))
			// No snapshotData could be found
			if err := ctrl.createSnapshot(snapshot); err != nil {
				return err
			}
			return nil
		} else /* snapshotData != nil */ {
			// Found a snapshotData for this snapshot
			glog.V(4).Infof("synchronizing unbound VolumeSnapshot[%s]: snapshot data %q found: %s", VsToVsKey(snapshot), snapshotData.Name)
			if err := ctrl.bind(snapshot, snapshotData); err != nil {
				// On any error saving the snapshotData or the snapshot, subsequent
				// syncSnapshot will finish the binding.
				return err
			}
			// OBSERVATION: snapshot is "Bound", snapshotData is "Bound"
			return nil
		}
	} else /* snapshot.Spec.SnapshotDataName != nil */ {
		// [Unit test set 2]
		// User asked for a specific snapshotData.
		glog.V(4).Infof("synchronizing unbound VolumeSnapshot[%s]: snapshotData %q requested", VsToVsKey(snapshot), snapshot.Spec.SnapshotDataName)
		obj, found, err := ctrl.snapshotDataStore.GetByKey(snapshot.Spec.SnapshotDataName)
		if err != nil {
			return err
		}
		if !found {
			// User asked for a snapshotData that does not exist.
			// OBSERVATION: VS is "Pending"
			// Retry later.
			glog.V(4).Infof("synchronizing unbound VolumeSnapshot[%s]: SnapshotData %q requested and not found, will try again next time", VsToVsKey(snapshot), snapshot.Spec.SnapshotDataName)
			condition := snapshotv1alpha1.VolumeSnapshotCondition{
				Type:    snapshotv1alpha1.VolumeSnapshotConditionCreating,
				Status:  v1.ConditionTrue,
				Message: "Requested SnapshotData not found",
			}

			if _, err := ctrl.snapshotConditionUpdate(snapshot, &condition); err != nil {
				return err
			}
			return nil
		} else {
			snapshotData, ok := obj.(*snapshotv1alpha1.VolumeSnapshotData)
			if !ok {
				return fmt.Errorf("cannot convert object from VolumeSnapshotData cache to VolumeSnapshotData %q!?: %+v", snapshot.Spec.SnapshotDataName, obj)
			}
			glog.V(4).Infof("synchronizing unbound VolumeSnapshot[%s]: SnapshotData %q requested and found", VsToVsKey(snapshot), snapshot.Spec.SnapshotDataName)
			if snapshotData.Spec.VolumeSnapshotRef == nil {
				// User asked for a snapshotData that is not bound
				if err = ctrl.bind(snapshot, snapshotData); err != nil {
					// On any error saving the snapshotData or the snapshot, subsequent
					// syncSnapshot will finish the binding.
					return err
				}
				// OBSERVATION: snapshot is "Bound", snapshotData is "Bound"
				return nil
			} else if isSnapshotDataBoundToSnapshot(snapshotData, snapshot) {
				// User asked for a snapshotData that is bound by this vs
				glog.V(4).Infof("synchronizing unbound VolumeSnapshot[%s]: SnapshotData already bound, finishing the binding", VsToVsKey(snapshot))

				// Finish the snapshotData binding by adding snapshot UID.
				if err = ctrl.bind(snapshot, snapshotData); err != nil {
					return err
				}
				// OBSERVATION: snapshot is "Bound", snapshotData is "Bound"
				return nil
			} else {
				// User asked for a snapshotData that is bound by someone else
				// OBSERVATION: snapshot is "Pending", snapshotData is "Bound"
				if !metav1.HasAnnotation(snapshot.ObjectMeta, annBoundByController) {
					glog.V(4).Infof("synchronizing unbound VolumeSnapshot[%s]: SnapshotData already bound to different snapshot by user, will retry later", VsToVsKey(snapshot))
					// User asked for a specific snapshotData, retry later
					condition := snapshotv1alpha1.VolumeSnapshotCondition{
						Type:    snapshotv1alpha1.VolumeSnapshotConditionCreating,
						Status:  v1.ConditionTrue,
						Message: "Requested SnapshotData is bound to other snapshot",
					}

					if _, err := ctrl.snapshotConditionUpdate(snapshot, &condition); err != nil {
						return err
					}
					return nil
				} else {
					// This should never happen because someone had to remove
					// annBindCompleted annotation on the snapshot.
					glog.V(4).Infof("synchronizing unbound VolumeSnapshot[%s]: SnapshotData already bound to different snapshot %q by controller, THIS SHOULD NEVER HAPPEN", VsToVsKey(snapshot), VsRefToVsKey(snapshotData.Spec.VolumeSnapshotRef))
					return fmt.Errorf("invalid binding of snapshot %q to SnapshotData %q: SnapshotData already bound by %q", VsToVsKey(snapshot), snapshot.Spec.SnapshotDataName, VsRefToVsKey(snapshotData.Spec.VolumeSnapshotRef))
				}
			}
		}
	}
}

// syncBoundSnapshot is the main controller method to decide what to do with a bound snapshot.
func (ctrl *CSISnapshotController) syncBoundSnapshot(snapshot *snapshotv1alpha1.VolumeSnapshot) error {
	// HasAnnotation(snapshot, annBindCompleted)
	// This snapshot has previously been bound
	if snapshot.Spec.SnapshotDataName == "" {
		// Snapshot was bound before but not any more.
		condition := &snapshotv1alpha1.VolumeSnapshotCondition{
			Type:    snapshotv1alpha1.VolumeSnapshotConditionError,
			Status:  v1.ConditionTrue,
			Reason:  "SnapshotLost",
			Message: "Bound snapshot has lost reference to VolumeSnapshotData. Data on the snapshot is lost!",
		}
		if _, err := ctrl.snapshotConditionUpdate(snapshot, condition); err != nil {
			return err
		}
		return nil
	}
	obj, found, err := ctrl.snapshotDataStore.GetByKey(snapshot.Spec.SnapshotDataName)
	if err != nil {
		return err
	}
	if !found {
		// Snapshot is bound to a non-existing snapshot data.
		condition := &snapshotv1alpha1.VolumeSnapshotCondition{
			Type:    snapshotv1alpha1.VolumeSnapshotConditionError,
			Status:  v1.ConditionTrue,
			Reason:  "SnapshotLost",
			Message: "Bound snapshot has lost its VolumeSnapshotData. Data on the snapshot is lost!",
		}
		if _, err := ctrl.snapshotConditionUpdate(snapshot, condition); err != nil {
			return err
		}
		return nil
	} else {
		snapshotData, ok := obj.(*snapshotv1alpha1.VolumeSnapshotData)
		if !ok {
			return fmt.Errorf("cannot convert object from snapshotData cache to snapshotData %q!?: %#v", snapshot.Spec.SnapshotDataName, obj)
		}

		glog.V(4).Infof("synchronizing bound VolumeSnapshot[%s]: snapshotData %q", VsToVsKey(snapshot), snapshot.Spec.SnapshotDataName)
		if snapshotData.Spec.VolumeSnapshotRef == nil {
			// VolumeSnapshot is bound but snapshotData has come unbound.
			// Or, a snapshot was bound and the controller has not received updated
			// snapshotData yet. We can't distinguish these cases.
			// Bind the snapshotData again.
			glog.V(4).Infof("synchronizing bound VolumeSnapshot[%s]: snapshotData is unbound, fixing", VsToVsKey(snapshot))
			if err = ctrl.bind(snapshot, snapshotData); err != nil {
				// Objects not saved, next syncSnapshotData or syncSnapshot will try again
				return err
			}
			return nil
		} else if snapshotData.Spec.VolumeSnapshotRef.UID == snapshot.UID {
			// All is well
			// everything should be already set.
			glog.V(4).Infof("synchronizing bound VolumeSnapshot[%s]: snapshot is already correctly bound", VsToVsKey(snapshot))

			if _, err := ctrl.syncCondition(snapshot, snapshotData); err != nil {
				// Objects not saved, next syncSnapshotData or syncSnapshot will try again
				return err
			}

			if err = ctrl.bind(snapshot, snapshotData); err != nil {
				// Objects not saved, next syncSnapshotData or syncSnapshot will try again
				return err
			}
			return nil
		} else {
			// Snapshot is bound but snapshotData has a different snapshot.
			condition := &snapshotv1alpha1.VolumeSnapshotCondition{
				Type:    snapshotv1alpha1.VolumeSnapshotConditionError,
				Status:  v1.ConditionTrue,
				Reason:  "SnapshotMisbound",
				Message: "Two snapshots are bound to the same snapshotData, this one is bound incorrectly!",
			}
			if _, err := ctrl.snapshotConditionUpdate(snapshot, condition); err != nil {
				return err
			}
			return nil
		}
	}
}

// findMatchSnapshotData goes through the list of volumeSnapshotData to one that is pre-bound or bound to the snapshot.
func (ctrl *CSISnapshotController) findMatchSnapshotData(snapshot *snapshotv1alpha1.VolumeSnapshot) *snapshotv1alpha1.VolumeSnapshotData {
	objs := ctrl.snapshotDataStore.List()
	for _, obj := range objs {
		snapshotData := obj.(*snapshotv1alpha1.VolumeSnapshotData)
		if isSnapshotDataBoundToSnapshot(snapshotData, snapshot) {
			glog.V(4).Infof("found VolumeSnapshotData %s that is bound to VolumeSnapshot %s", snapshotData.Name, VsToVsKey(snapshot))
			return snapshotData
		}
	}
	return nil
}

// unbindSnapshotData rolls back previous binding of the snapshotData. This may be necessary
// when two controllers bound two snapshotDatas to single snapshot - when we detect this,
// only one binding succeeds and the second one must be rolled back.
// This method updates both Spec and Status.
// It returns on first error, it's up to the caller to implement some retry
// mechanism.
func (ctrl *CSISnapshotController) unbindSnapshotData(snapshotData *snapshotv1alpha1.VolumeSnapshotData) error {
	glog.V(4).Infof("updating VolumeSnapshotData[%s]: rolling back binding from %q", snapshotData.Name, VsRefToVsKey(snapshotData.Spec.VolumeSnapshotRef))

	// Save the VSD only when any modification is necessary.
	snapshotDataClone := snapshotData.DeepCopy()

	if metav1.HasAnnotation(snapshotData.ObjectMeta, annBoundByController) {
		// The snapshotData was bound by the controller.
		snapshotDataClone.Spec.VolumeSnapshotRef = nil
		delete(snapshotDataClone.Annotations, annBoundByController)
		if len(snapshotDataClone.Annotations) == 0 {
			// No annotations look better than empty annotation map (and it's easier
			// to test).
			snapshotDataClone.Annotations = nil
		}
	} else {
		// The snapshotData was pre-bound by user. Clear only the binging UID.
		snapshotDataClone.Spec.VolumeSnapshotRef.UID = ""
	}

	newVsd, err := ctrl.snapshotClient.VolumesnapshotV1alpha1().VolumeSnapshotDatas().Update(snapshotDataClone)
	if err != nil {
		glog.V(4).Infof("updating VolumeSnapshotData[%s]: rollback failed: %v", snapshotData.Name, err)
		return err
	}
	_, err = ctrl.storeSnapshotDataUpdate(newVsd)
	if err != nil {
		glog.V(4).Infof("updating VolumeSnapshotData[%s]: cannot update internal cache: %v", snapshotData.Name, err)
		return err
	}
	glog.V(4).Infof("updating VolumeSnapshotData[%s]: rolled back", newVsd.Name)

	return nil
}

// bind saves binding information both to the snapshotData and the snapshot and marks
// both objects as Bound. snapshotData is saved first.
// It returns on first error, it's up to the caller to implement some retry
// mechanism.
func (ctrl *CSISnapshotController) bind(snapshot *snapshotv1alpha1.VolumeSnapshot, snapshotData *snapshotv1alpha1.VolumeSnapshotData) error {
	var err error
	// use updatedSnapshot/updatedSnapshotData to keep the original snapshot/snapshotData for
	// logging in error cases.
	var updatedSnapshot *snapshotv1alpha1.VolumeSnapshot
	var updatedSnapshotData *snapshotv1alpha1.VolumeSnapshotData

	glog.V(4).Infof("binding snapshotData %q to snapshot %q", snapshotData.Name, VsToVsKey(snapshot))

	if updatedSnapshotData, err = ctrl.bindSnapshotDataToSnapshot(snapshotData, snapshot); err != nil {
		glog.V(3).Infof("error binding snapshotData %q to snapshot %q: failed saving the snapshotData: %v", snapshotData.Name, VsToVsKey(snapshot), err)
		return err
	}
	snapshotData = updatedSnapshotData

	if updatedSnapshot, err = ctrl.bindSnapshotToSnapshotData(snapshot, snapshotData); err != nil {
		glog.V(3).Infof("error binding snapshotData %q to snapshot %q: failed saving the snapshot: %v", snapshotData.Name, VsToVsKey(snapshot), err)
		return err
	}
	snapshot = updatedSnapshot

	glog.V(4).Infof("snapshotData %q bound to snapshot %q", snapshotData.Name, VsToVsKey(snapshot))
	return nil
}

// bindSnapshotToSnapshotData modifies the given snapshot to be bound to SnapshotData and
// saves it to API server. The SnapshotData is not modified in this method!
func (ctrl *CSISnapshotController) bindSnapshotToSnapshotData(snapshot *snapshotv1alpha1.VolumeSnapshot, snapshotData *snapshotv1alpha1.VolumeSnapshotData) (*snapshotv1alpha1.VolumeSnapshot, error) {
	glog.V(4).Infof("updating VolumeSnapshot[%s]: binding to %q", VsToVsKey(snapshot), snapshotData.Name)

	dirty := false

	// Check if the snapshot was already bound (either by controller or by user)
	shouldBind := false
	if snapshotData.Name != snapshot.Spec.SnapshotDataName {
		shouldBind = true
	}

	// The snapshot from method args can be pointing to watcher cache. We must not
	// modify these, therefore create a copy.
	snapshotClone := snapshot.DeepCopy()

	if shouldBind {
		dirty = true
		// Bind the snapshot to the snapshotData
		snapshotClone.Spec.SnapshotDataName = snapshotData.Name

		// Set annBoundByController if it is not set yet
		if !metav1.HasAnnotation(snapshotClone.ObjectMeta, annBoundByController) {
			metav1.SetMetaDataAnnotation(&snapshotClone.ObjectMeta, annBoundByController, "yes")
		}
	}

	// Set annBindCompleted if it is not set yet
	if !metav1.HasAnnotation(snapshotClone.ObjectMeta, annBindCompleted) {
		metav1.SetMetaDataAnnotation(&snapshotClone.ObjectMeta, annBindCompleted, "yes")
		dirty = true
	}

	if dirty {
		glog.V(2).Infof("snapshotData %q bound to snapshot %q", snapshotData.Name, VsToVsKey(snapshot))
		newSnapshot, err := ctrl.snapshotClient.VolumesnapshotV1alpha1().VolumeSnapshots(snapshot.Namespace).Update(snapshotClone)
		if err != nil {
			glog.V(4).Infof("updating VolumeSnapshot[%s]: binding to %q failed: %v", VsToVsKey(snapshot), snapshotData.Name, err)
			return newSnapshot, err
		}
		_, err = ctrl.storeSnapshotUpdate(snapshotClone)
		if err != nil {
			glog.V(4).Infof("updating VolumeSnapshot[%s]: cannot update internal cache: %v", VsToVsKey(snapshot), err)
			return newSnapshot, err
		}
		glog.V(4).Infof("updating VolumeSnapshot[%s]: bound to %q", VsToVsKey(snapshot), snapshotData.Name)
		return newSnapshot, nil
	}

	glog.V(4).Infof("updating VolumeSnapshot[%s]: already bound to %q", VsToVsKey(snapshot), snapshotData.Name)
	return snapshot, nil
}

// bindSnapshotDataToSnapshot modifies given SnapshotData to be bound to a Snapshot and saves it to
// API server. The Snapshot is not modified in this method!
func (ctrl *CSISnapshotController) bindSnapshotDataToSnapshot(snapshotData *snapshotv1alpha1.VolumeSnapshotData, snapshot *snapshotv1alpha1.VolumeSnapshot) (*snapshotv1alpha1.VolumeSnapshotData, error) {
	glog.V(4).Infof("bindSnapshotDataToSnapshot[%s]: binding to %q", snapshotData.Name, VsToVsKey(snapshot))

	snapshotDataClone, dirty, err := ctrl.getBindSnapshotDataToSnapshot(snapshotData, snapshot)
	if err != nil {
		return nil, err
	}

	// Save the snapshotData only if something was changed
	if dirty {
		return ctrl.updateBindSnapshotDataToSnapshot(snapshotDataClone, snapshot, true)
	}

	glog.V(4).Infof("bindSnapshotDataToSnapshot[%s]: already bound to %q", snapshotData.Name, VsToVsKey(snapshot))
	return snapshotData, nil
}

// updateBindSnapshotDataToSnapshot modifies given snapshotData to be bound to a snapshot and saves it to
// API server. The snapshot is not modified in this method!
func (ctrl *CSISnapshotController) updateBindSnapshotDataToSnapshot(snapshotDataClone *snapshotv1alpha1.VolumeSnapshotData, snapshot *snapshotv1alpha1.VolumeSnapshot, updateCache bool) (*snapshotv1alpha1.VolumeSnapshotData, error) {
	glog.V(2).Infof("snapshot %q bound to snapshotData %q", VsToVsKey(snapshot), snapshotDataClone.Name)
	newSnapshotData, err := ctrl.snapshotClient.VolumesnapshotV1alpha1().VolumeSnapshotDatas().Update(snapshotDataClone)
	if err != nil {
		glog.V(4).Infof("updating VolumeSnapshotData[%s]: binding to %q failed: %v", snapshotDataClone.Name, VsToVsKey(snapshot), err)
		return newSnapshotData, err
	}
	if updateCache {
		_, err = ctrl.storeSnapshotDataUpdate(newSnapshotData)
		if err != nil {
			glog.V(4).Infof("updating VolumeSnapshotData[%s]: cannot update internal cache: %v", newSnapshotData.Name, err)
			return newSnapshotData, err
		}
	}
	glog.V(4).Infof("updating VolumeSnapshotData[%s]: bound to %q", newSnapshotData.Name, VsToVsKey(snapshot))
	return newSnapshotData, nil
}

// Get new snapshotData object only, no API or cache update
func (ctrl *CSISnapshotController) getBindSnapshotDataToSnapshot(snapshotData *snapshotv1alpha1.VolumeSnapshotData, snapshot *snapshotv1alpha1.VolumeSnapshot) (*snapshotv1alpha1.VolumeSnapshotData, bool, error) {
	dirty := false

	// Check if the snapshotData was already bound (either by user or by controller)
	shouldSetBoundByController := false
	if !isSnapshotDataBoundToSnapshot(snapshotData, snapshot) {
		shouldSetBoundByController = true
	}

	// The snapshotData from method args can be pointing to watcher cache. We must not
	// modify these, therefore create a copy.
	snapshotDataClone := snapshotData.DeepCopy()

	// Bind the snapshotData to the snapshot if it is not bound yet
	if snapshotData.Spec.VolumeSnapshotRef == nil ||
		snapshotData.Spec.VolumeSnapshotRef.Name != snapshot.Name ||
		snapshotData.Spec.VolumeSnapshotRef.Namespace != snapshot.Namespace ||
		snapshotData.Spec.VolumeSnapshotRef.UID != snapshot.UID {

		snapshotRef, err := ref.GetReference(scheme.Scheme, snapshot)
		if err != nil {
			return nil, false, fmt.Errorf("unexpected error getting snapshot reference: %v", err)
		}
		snapshotDataClone.Spec.VolumeSnapshotRef = snapshotRef
		dirty = true
	}

	// Set annBoundByController if it is not set yet
	if shouldSetBoundByController && !metav1.HasAnnotation(snapshotDataClone.ObjectMeta, annBoundByController) {
		metav1.SetMetaDataAnnotation(&snapshotDataClone.ObjectMeta, annBoundByController, "yes")
		dirty = true
	}

	return snapshotDataClone, dirty, nil
}

func (ctrl *CSISnapshotController) deleteSnapshotDataOperation(snapshotData *snapshotv1alpha1.VolumeSnapshotData) error {
	glog.V(4).Infof("deleteSnapshotOperation [%s] started", snapshotData.Name)

	newSnapshotData, err := ctrl.snapshotClient.VolumesnapshotV1alpha1().VolumeSnapshotDatas().Get(snapshotData.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get snapshot data from apiserver %#v, err: %v", snapshotData.Name, err)
	}

	err = ctrl.handler.deleteSnapshot(newSnapshotData)
	if err != nil {
		ctrl.eventRecorder.Event(newSnapshotData, v1.EventTypeWarning, "SnapshotDataFailedDelete", err.Error())
		return fmt.Errorf("failed to delete snapshot %#v, err: %v", newSnapshotData.Name, err)
	}

	glog.Infof("snapshot data %#v deleted", snapshotData)

	err = ctrl.snapshotClient.VolumesnapshotV1alpha1().VolumeSnapshotDatas().Delete(snapshotData.Name, &metav1.DeleteOptions{})
	if err != nil {
		// Oops, could not delete the snapshot data and therefore the controller will
		// try to delete the snapshot data again on next update. We _could_ maintain a
		// cache of "recently deleted snapshot data" and avoid unnecessary deletion,
		// this is left out as future optimization.
		glog.V(3).Infof("failed to delete snapshot data %q from database: %v", snapshotData.Name, err)
		return nil
	}

	return nil
}

// scheduleOperation starts given asynchronous operation on given snapshot. It
// makes sure the operation is already not running.
func (ctrl *CSISnapshotController) scheduleOperation(operationName string, operation func() error) {
	glog.V(4).Infof("scheduleOperation[%s]", operationName)

	err := ctrl.runningOperations.Run(operationName, operation)
	if err != nil {
		switch {
		case goroutinemap.IsAlreadyExists(err):
			glog.V(4).Infof("operation %q is already running, skipping", operationName)
		case exponentialbackoff.IsExponentialBackoff(err):
			glog.V(4).Infof("operation %q postponed due to exponential backoff", operationName)
		default:
			glog.Errorf("error scheduling operation %q: %v", operationName, err)
		}
	}
}

// createSnapshot starts new asynchronous operation to create snapshot data for snapshot
func (ctrl *CSISnapshotController) createSnapshot(snapshot *snapshotv1alpha1.VolumeSnapshot) error {
	glog.V(4).Infof("createSnapshot[%s]: started", VsToVsKey(snapshot))
	opName := fmt.Sprintf("create-%s[%s]", VsToVsKey(snapshot), string(snapshot.UID))
	ctrl.scheduleOperation(opName, func() error {
		ctrl.createSnapshotOperation(snapshot)
		return nil
	})
	return nil
}

// createSnapshotOperation create a VolumeSnapshotData. This method is running in
// standalone goroutine and already has all necessary locks.
func (ctrl *CSISnapshotController) createSnapshotOperation(snapshot *snapshotv1alpha1.VolumeSnapshot) {
	glog.V(4).Infof("createSnapshotOperation [%s] started", VsToVsKey(snapshot))

	//  A previous createSnapshot may just have finished while we were waiting for
	//  the locks. Check that snapshot data (with deterministic name) hasn't been created
	//  yet.
	snapDataName := GetSnapshotDataNameForSnapshot(snapshot)
	snapshotData, err := ctrl.snapshotClient.VolumesnapshotV1alpha1().VolumeSnapshotDatas().Get(snapDataName, metav1.GetOptions{})
	if err == nil && snapshotData != nil {
		// Volume snapshot data has been already created, nothing to do.
		glog.V(4).Infof("createSnapshot [%s]: volume snapshot data already exists, skipping", VsToVsKey(snapshot))
		return
	}

	volume, err := ctrl.getVolumeFromVolumeSnapshot(snapshot)
	if err != nil {
		glog.V(3).Infof(err.Error())
		return
	}

	// Prepare a volumeSnapshotRef and persistentVolumeRef early (to fail before a snapshotData is
	// created)
	volumeSnapshotRef, err := ref.GetReference(scheme.Scheme, snapshot)
	if err != nil {
		glog.V(3).Infof("unexpected error getting snapshot reference: %v", err)
		return
	}
	persistentVolumeRef, err := ref.GetReference(scheme.Scheme, volume)
	if err != nil {
		glog.V(3).Infof("unexpected error getting volume reference: %v", err)
		return
	}

	class, err := ctrl.snapshotClassLister.Get(snapshot.Spec.SnapshotClassName)
	if err != nil {
		glog.V(3).Infof("unexpected error getting snapshot class: %v", err)
		return
	}

	if class.Snapshotter != ctrl.snapshotter {
		glog.Errorf("Unknown Snapshotter %q requested in snapshot %q's SnapshotClass %q", class.Snapshotter, VsToVsKey(snapshot), class)
		return
	}

	snapshotData, err = ctrl.handler.takeSnapshot(snapshot, volume, class.Parameters)
	if err != nil {
		strerr := fmt.Sprintf("Failed to create snapshot data with SnapshotClass %q: %v", class.Name, err)
		glog.V(2).Infof("failed to create snapshot data for snapshot %q with SnapshotClass %q: %v", VsToVsKey(snapshot), class.Name, err)
		ctrl.eventRecorder.Event(snapshot, v1.EventTypeWarning, CreateSnapshotFailed, strerr)
		return
	}

	glog.V(3).Infof("VolumeSnapshotData %q for VolumeSnapshot %q created", snapshotData.Name, VsToVsKey(snapshot))

	// Bind it to the VolumeSnapshot
	snapshotData.Spec.VolumeSnapshotRef = volumeSnapshotRef
	snapshotData.Spec.PersistentVolumeRef = persistentVolumeRef

	// Add annBoundByController (used in deleting the VolumeSnapshotData)
	metav1.SetMetaDataAnnotation(&snapshotData.ObjectMeta, annBoundByController, "yes")
	metav1.SetMetaDataAnnotation(&snapshotData.ObjectMeta, annDynamicallyCreatedBy, class.Snapshotter)

	// Try to create the VSD object several times
	for i := 0; i < ctrl.createSnapshotDataRetryCount; i++ {
		glog.V(4).Infof("createSnapshot [%s]: trying to save volume snapshot data %s", VsToVsKey(snapshot), snapshotData.Name)
		if _, err = ctrl.snapshotClient.VolumesnapshotV1alpha1().VolumeSnapshotDatas().Create(snapshotData); err == nil || apierrs.IsAlreadyExists(err) {
			// Save succeeded.
			if err != nil {
				glog.V(3).Infof("volume snapshot data %q for snapshot %q already exists, reusing", snapshotData.Name, VsToVsKey(snapshot))
				err = nil
			} else {
				glog.V(3).Infof("volume snapshot data %q for snapshot %q saved", snapshotData.Name, VsToVsKey(snapshot))
			}
			break
		}
		// Save failed, try again after a while.
		glog.V(3).Infof("failed to save volume snapshot data %q for snapshot %q: %v", snapshotData.Name, VsToVsKey(snapshot), err)
		time.Sleep(ctrl.createSnapshotDataInterval)
	}

	if err != nil {
		// Save failed. Now we have a storage asset outside of Kubernetes,
		// but we don't have appropriate snapshotdata object for it.
		// Emit some event here and try to delete the storage asset several
		// times.
		strerr := fmt.Sprintf("Error creating volume snapshot data object for snapshot %s: %v. Deleting the snapshot data.", VsToVsKey(snapshot), err)
		glog.V(3).Info(strerr)
		ctrl.eventRecorder.Event(snapshot, v1.EventTypeWarning, "CreateSnapshotDataFailed", strerr)

		for i := 0; i < ctrl.createSnapshotDataRetryCount; i++ {
			if err = ctrl.deleteSnapshotDataOperation(snapshotData); err == nil {
				// Delete succeeded
				glog.V(4).Infof("createSnapshot [%s]: cleaning snapshot data %s succeeded", VsToVsKey(snapshot), snapshotData.Name)
				break
			}
			// Delete failed, try again after a while.
			glog.Infof("failed to delete snapshot data %q: %v", snapshotData.Name, err)
			time.Sleep(ctrl.createSnapshotDataInterval)
		}

		if err != nil {
			// Delete failed several times. There is an orphaned volume snapshot data and there
			// is nothing we can do about it.
			strerr := fmt.Sprintf("Error cleaning volume snapshot data for snapshot %s: %v. Please delete manually.", VsToVsKey(snapshot), err)
			glog.Error(strerr)
			ctrl.eventRecorder.Event(snapshot, v1.EventTypeWarning, SnapshotDataCleanupFailed, strerr)
		}
	} else {
		glog.V(2).Infof("VolumeSnapshotData %q created for VolumeSnapshot %q", snapshotData.Name, VsToVsKey(snapshot))
		msg := fmt.Sprintf("Successfully create snapshot data  %s using %s", snapshotData.Name, class.Name)
		ctrl.eventRecorder.Event(snapshot, v1.EventTypeNormal, CreateSnapshotSucceeded, msg)
	}
}

// getClaimFromVolumeSnapshot is a helper function to get claim from VolumeSnapshot.
func (ctrl *CSISnapshotController) getClaimFromVolumeSnapshot(snapshot *snapshotv1alpha1.VolumeSnapshot) (*v1.PersistentVolumeClaim, error) {
	pvcName := snapshot.Spec.PersistentVolumeClaimName
	if pvcName == "" {
		return nil, fmt.Errorf("the PVC name is not specified in snapshot %s", VsToVsKey(snapshot))
	}

	pvc, err := ctrl.claimLister.PersistentVolumeClaims(snapshot.Namespace).Get(pvcName)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve PVC %s from the API server: %q", pvcName, err)
	}
	if pvc.Status.Phase != v1.ClaimBound {
		return nil, fmt.Errorf("the PVC %s not yet bound to a PV, will not attempt to take a snapshot yet", pvcName)
	}

	return pvc, nil
}

// getVolumeFromVolumeSnapshot is a helper function to get PV from VolumeSnapshot.
func (ctrl *CSISnapshotController) getVolumeFromVolumeSnapshot(snapshot *snapshotv1alpha1.VolumeSnapshot) (*v1.PersistentVolume, error) {
	pvc, err := ctrl.getClaimFromVolumeSnapshot(snapshot)
	if err != nil {
		return nil, err
	}

	pvName := pvc.Spec.VolumeName
	pv, err := ctrl.volumeLister.Get(pvName)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve PV %s from the API server: %q", pvName, err)
	}
	return pv, nil
}

// syncCondition syncs condition between snapshot and snapshotData.
func (ctrl *CSISnapshotController) syncCondition(snapshot *snapshotv1alpha1.VolumeSnapshot, snapshotData *snapshotv1alpha1.VolumeSnapshotData) (*snapshotv1alpha1.VolumeSnapshot, error) {
	conditions := snapshotData.Status.Conditions
	if conditions == nil || len(conditions) == 0 {
		glog.V(4).Infof("syncCondition: snapshotData %v condition is empty, no need to sync snapshot %v", snapshotData.Name, VsToVsKey(snapshot))
		return snapshot, nil
	}

	lastCondition := conditions[len(conditions)-1]

	return ctrl.snapshotConditionUpdate(snapshot, &lastCondition)
}

func (ctrl *CSISnapshotController) snapshotConditionUpdate(snapshot *snapshotv1alpha1.VolumeSnapshot, condition *snapshotv1alpha1.VolumeSnapshotCondition) (*snapshotv1alpha1.VolumeSnapshot, error) {
	if condition == nil {
		return snapshot, nil
	}

	snapshotCopy := snapshot.DeepCopy()

	// no condition changes.
	if !UpdateSnapshotCondition(&snapshotCopy.Status, condition) {
		return snapshotCopy, nil
	}

	glog.V(2).Infof("Updating snapshot condition for %s/%s to (%s==%s)", snapshot.Namespace, snapshot.Name, condition.Type, condition.Status)

	newVs, err := ctrl.snapshotClient.VolumesnapshotV1alpha1().VolumeSnapshots(snapshot.Namespace).UpdateStatus(snapshotCopy)
	if err != nil {
		glog.V(4).Infof("updating VolumeSnapshot[%s] status failed: %v", VsToVsKey(snapshot), err)
		return newVs, err
	}
	_, err = ctrl.storeSnapshotUpdate(newVs)
	if err != nil {
		glog.V(4).Infof("updating VolumeSnapshot[%s] status: cannot update internal cache: %v", VsToVsKey(snapshot), err)
		return newVs, err
	}

	glog.V(2).Infof("VolumeSnapshot %q status update success", VsToVsKey(snapshot))
	return newVs, nil
}

func (ctrl *CSISnapshotController) snapshotDataConditionUpdate(snapshotData *snapshotv1alpha1.VolumeSnapshotData, condition *snapshotv1alpha1.VolumeSnapshotCondition) (*snapshotv1alpha1.VolumeSnapshotData, error) {
	if condition == nil {
		return snapshotData, nil
	}

	snapshotDataCopy := snapshotData.DeepCopy()

	// no condition changes.
	if !UpdateSnapshotCondition(&snapshotDataCopy.Status, condition) {
		return snapshotDataCopy, nil
	}

	glog.V(2).Infof("Updating snapshotData condition for %s to (%s==%s)", snapshotData.Name, condition.Type, condition.Status)

	newVsd, err := ctrl.snapshotClient.VolumesnapshotV1alpha1().VolumeSnapshotDatas().UpdateStatus(snapshotDataCopy)
	if err != nil {
		glog.V(4).Infof("updating VolumeSnapshotData[%s] status failed: %v", newVsd.Name, err)
		return newVsd, err
	}
	_, err = ctrl.storeSnapshotDataUpdate(newVsd)
	if err != nil {
		glog.V(4).Infof("updating VolumeSnapshotData[%s] status: cannot update internal cache: %v", snapshotData.Name, err)
		return newVsd, err
	}

	glog.V(2).Infof("VolumeSnapshotData %q status update success", snapshotData.Name)
	return newVsd, nil
}

func (ctrl *CSISnapshotController) shouldEnqueueSnapshot(snapshot *snapshotv1alpha1.VolumeSnapshot) bool {
	snapshotClass, err := ctrl.snapshotClassLister.Get(snapshot.Spec.SnapshotClassName)
	if err != nil {
		glog.Errorf("Error getting snapshot %q's StorageClass: %v", VsToVsKey(snapshot), err)
		return false
	}

	if snapshotClass.Snapshotter != ctrl.snapshotter {
		return false
	}

	return true
}

func (ctrl *CSISnapshotController) shouldEnqueueSnapshotData(snapshotData *snapshotv1alpha1.VolumeSnapshotData) bool {
	if !metav1.HasAnnotation(snapshotData.ObjectMeta, annDynamicallyCreatedBy) {
		return false
	}

	if ann := snapshotData.Annotations[annDynamicallyCreatedBy]; ann != ctrl.snapshotter {
		return false
	}

	return true
}
