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

package v1alpha1

import (
	"encoding/json"

	core_v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	// VolumeSnapshotDataResourcePlural is "volumesnapshotdatas"
	VolumeSnapshotDataResourcePlural = "volumesnapshotdatas"
	// VolumeSnapshotResourcePlural is "volumesnapshots"
	VolumeSnapshotResourcePlural = "volumesnapshots"
	// SnapshotClassResourcePlural is "snapshotclasss"
	SnapshotClassResourcePlural = "snapshotclasses"
)

// VolumeSnapshotStatus is the status of the VolumeSnapshot
type VolumeSnapshotStatus struct {
	// The time the snapshot was successfully created
	// +optional
	CreationTimestamp metav1.Time `json:"creationTimestamp" protobuf:"bytes,1,opt,name=creationTimestamp"`

	// Represent the latest available observations about the volume snapshot
	Conditions []VolumeSnapshotCondition `json:"conditions" protobuf:"bytes,2,rep,name=conditions"`
}

// VolumeSnapshotConditionType is the type of VolumeSnapshot conditions
type VolumeSnapshotConditionType string

// These are valid conditions of a volume snapshot.
const (
	// VolumeSnapshotConditionCreating means the snapshot is being created but
	// it is not cut yet.
	VolumeSnapshotConditionCreating VolumeSnapshotConditionType = "Creating"
	// VolumeSnapshotConditionUploading means the snapshot is cut and the application
	// can resume accessing data if ConditionStatus is True. It corresponds
	// to "Uploading" in GCE PD or "Pending" in AWS and ConditionStatus is True.
	// This condition type is not applicable in OpenStack Cinder.
	VolumeSnapshotConditionUploading VolumeSnapshotConditionType = "Uploading"
	// VolumeSnapshotConditionReady is added when the snapshot has been successfully created and is ready to be used.
	VolumeSnapshotConditionReady VolumeSnapshotConditionType = "Ready"
	// VolumeSnapshotConditionError means an error occurred during snapshot creation.
	VolumeSnapshotConditionError VolumeSnapshotConditionType = "Error"
)

// VolumeSnapshotCondition describes the state of a volume snapshot  at a certain point.
type VolumeSnapshotCondition struct {
	// Type of replication controller condition.
	Type VolumeSnapshotConditionType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=VolumeSnapshotConditionType"`
	// Status of the condition, one of True, False, Unknown.
	Status core_v1.ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status,casttype=ConditionStatus"`
	// The last time the condition transitioned from one status to another.
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime" protobuf:"bytes,3,opt,name=lastTransitionTime"`
	// The reason for the condition's last transition.
	// +optional
	Reason string `json:"reason" protobuf:"bytes,4,opt,name=reason"`
	// A human readable message indicating details about the transition.
	// +optional
	Message string `json:"message" protobuf:"bytes,5,opt,name=message"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VolumeSnapshot is the volume snapshot object accessible to the user. Upon successful creation of the actual
// snapshot by the volume provider it is bound to the corresponding VolumeSnapshotData through
// the VolumeSnapshotSpec
type VolumeSnapshot struct {
	metav1.TypeMeta `json:",inline"`
	// Standard object's metadata.
	// More info: https://git.k8s.io/community/contributors/devel/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec represents the desired state of the snapshot
	// +optional
	Spec VolumeSnapshotSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`

	// Status represents the latest observer state of the snapshot
	// +optional
	Status VolumeSnapshotStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VolumeSnapshotList is a list of VolumeSnapshot objects
type VolumeSnapshotList struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Items []VolumeSnapshot `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// VolumeSnapshotSpec is the desired state of the volume snapshot
type VolumeSnapshotSpec struct {
	// PersistentVolumeClaimName is the name of the PVC being snapshotted
	// +optional
	PersistentVolumeClaimName string `json:"persistentVolumeClaimName" protobuf:"bytes,1,opt,name=persistentVolumeClaimName"`

	// SnapshotDataName binds the VolumeSnapshot object with the VolumeSnapshotData
	// +optional
	SnapshotDataName string `json:"snapshotDataName" protobuf:"bytes,2,opt,name=snapshotDataName"`

	// Name of the SnapshotClass required by the volume snapshot.
	// +optional
	SnapshotClassName string `json:"snapshotClassName" protobuf:"bytes,3,opt,name=snapshotClassName"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SnapshotClass describes the parameters used by storage system when
// provisioning VolumeSnapshots from PVCs.
//
// SnapshotClasses are non-namespaced; the name of the snapshot class
// according to etcd is in ObjectMeta.Name.
type SnapshotClass struct {
	metav1.TypeMeta `json:",inline"`
	// Standard object's metadata.
	// More info: https://git.k8s.io/community/contributors/devel/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Snapshotter is the driver expected to handle this SnapshotClass.
	Snapshotter string `json:"snapshotter" protobuf:"bytes,2,opt,name=snapshotter"`

	// Parameters holds parameters for the snapshotter.
	// These values are opaque to the system and are passed directly
	// to the snapshotter.
	// +optional
	Parameters map[string]string `json:"parameters,omitempty" protobuf:"bytes,3,rep,name=parameters"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SnapshotClassList is a collection of snapshot classes.
type SnapshotClassList struct {
	metav1.TypeMeta `json:",inline"`
	// Standard list metadata
	// More info: https://git.k8s.io/community/contributors/devel/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items is the list of SnapshotClasses
	Items []SnapshotClass `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VolumeSnapshotData represents the actual "on-disk" snapshot object
type VolumeSnapshotData struct {
	metav1.TypeMeta `json:",inline"`
	// Standard object's metadata.
	// More info: https://git.k8s.io/community/contributors/devel/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec represents the desired state of the snapshot
	// +optional
	Spec VolumeSnapshotDataSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VolumeSnapshotDataList is a list of VolumeSnapshotData objects
type VolumeSnapshotDataList struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Items []VolumeSnapshotData `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// VolumeSnapshotDataSpec is the spec of the volume snapshot data
type VolumeSnapshotDataSpec struct {
	// Source represents the location and type of the volume snapshot
	VolumeSnapshotSource `json:",inline" protobuf:"bytes,1,opt,name=volumeSnapshotSource"`

	// VolumeSnapshotRef is part of bi-directional binding between VolumeSnapshot
	// and VolumeSnapshotData
	// +optional
	VolumeSnapshotRef *core_v1.ObjectReference `json:"volumeSnapshotRef" protobuf:"bytes,2,opt,name=volumeSnapshotRef"`

	// PersistentVolumeRef represents the PersistentVolume that the snapshot has been
	// taken from
	// +optional
	PersistentVolumeRef *core_v1.ObjectReference `json:"persistentVolumeRef" protobuf:"bytes,3,opt,name=persistentVolumeRef"`
}

// VolumeSnapshotSource represents the actual location and type of the snapshot. Only one of its members may be specified.
type VolumeSnapshotSource struct {
	// CSI (Container Storage Interface) represents storage that handled by an external CSI Volume Driver (Alpha feature).
	// +optional
	CSI *CSIVolumeSnapshotSource `json:"csiVolumeSnapshotSource,omitempty"`
}

// Represents the source from CSI volume snapshot
type CSIVolumeSnapshotSource struct {
	// Driver is the name of the driver to use for this snapshot.
	// Required.
	Driver string `json:"driver"`

	// SnapshotHandle is the unique snapshot id returned by the CSI volume
	// plugin’s CreateSnapshot to refer to the snapshot on all subsequent calls.
	// Required.
	SnapshotHandle string `json:"snapshotHandle"`

	// Timestamp when the point-in-time snapshot is taken on the storage
	// system. The format of this field should be a Unix nanoseconds time
	// encoded as an int64. On Unix, the command `date +%s%N` returns
	// the  current time in nanoseconds since 1970-01-01 00:00:00 UTC.
	// This field is REQUIRED.
	CreatedAt int64 `json:"createdAt,omitempty" protobuf:"varint,2,opt,name=createdAt"`
}

// GetObjectKind is required to satisfy Object interface
func (v *VolumeSnapshotData) GetObjectKind() schema.ObjectKind {
	return &v.TypeMeta
}

// GetObjectMeta is required to satisfy ObjectMetaAccessor interface
func (v *VolumeSnapshotData) GetObjectMeta() metav1.Object {
	return &v.ObjectMeta
}

// GetObjectKind is required to satisfy Object interface
func (vd *VolumeSnapshotDataList) GetObjectKind() schema.ObjectKind {
	return &vd.TypeMeta
}

// GetListMeta is required to satisfy ListMetaAccessor interface
//func (vd *VolumeSnapshotDataList) GetListMeta() metav1.ListInterface {
//	return &vd.ListMeta
//}

// GetObjectKind is required to satisfy Object interface
func (v *VolumeSnapshot) GetObjectKind() schema.ObjectKind {
	return &v.TypeMeta
}

// GetObjectMeta is required to satisfy ObjectMetaAccessor interface
func (v *VolumeSnapshot) GetObjectMeta() metav1.Object {
	return &v.ObjectMeta
}

// GetObjectKind is required to satisfy Object interface
func (vd *VolumeSnapshotList) GetObjectKind() schema.ObjectKind {
	return &vd.TypeMeta
}

// GetListMeta is required to satisfy ListMetaAccessor interface
//func (vd *VolumeSnapshotList) GetListMeta() metav1.ListInterface {
//	return &vd.ListMeta
//}

// VolumeSnapshotDataListCopy is a VolumeSnapshotDataList type
type VolumeSnapshotDataListCopy VolumeSnapshotDataList

// VolumeSnapshotDataCopy is a VolumeSnapshotData type
type VolumeSnapshotDataCopy VolumeSnapshotData

// VolumeSnapshotListCopy is a VolumeSnapshotList type
type VolumeSnapshotListCopy VolumeSnapshotList

// VolumeSnapshotCopy is a VolumeSnapshot type
type VolumeSnapshotCopy VolumeSnapshot

// UnmarshalJSON unmarshalls json data
func (v *VolumeSnapshot) UnmarshalJSON(data []byte) error {
	tmp := VolumeSnapshotCopy{}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	tmp2 := VolumeSnapshot(tmp)
	*v = tmp2
	return nil
}

// UnmarshalJSON unmarshals json data
func (vd *VolumeSnapshotList) UnmarshalJSON(data []byte) error {
	tmp := VolumeSnapshotListCopy{}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	tmp2 := VolumeSnapshotList(tmp)
	*vd = tmp2
	return nil
}

// UnmarshalJSON unmarshals json data
func (v *VolumeSnapshotData) UnmarshalJSON(data []byte) error {
	tmp := VolumeSnapshotDataCopy{}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	tmp2 := VolumeSnapshotData(tmp)
	*v = tmp2
	return nil
}

// UnmarshalJSON unmarshals json data
func (vd *VolumeSnapshotDataList) UnmarshalJSON(data []byte) error {
	tmp := VolumeSnapshotDataListCopy{}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	tmp2 := VolumeSnapshotDataList(tmp)
	*vd = tmp2
	return nil
}
