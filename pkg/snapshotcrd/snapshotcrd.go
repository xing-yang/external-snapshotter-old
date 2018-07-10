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

package snapshotcrd

import (
	"fmt"
	"reflect"
	"time"

	"github.com/golang/glog"
	snapshotv1alpha1 "github.com/kubernetes-csi/external-snapshotter/pkg/apis/volumesnapshot/v1alpha1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

const (
	// SnapshotPVCAnnotation is "snapshot.alpha.kubernetes.io/snapshot"
	SnapshotPVCAnnotation = "snapshot.alpha.kubernetes.io/snapshot"
)

func CreateAndWaitForSnapshotCRD(clientset apiextensionsclient.Interface) error {
	crds := []struct {
		name   string
		plural string
		kind   string
		scope  apiextensionsv1beta1.ResourceScope
	}{
		{
			name:   snapshotv1alpha1.VolumeSnapshotResourcePlural + "." + snapshotv1alpha1.GroupName,
			plural: snapshotv1alpha1.VolumeSnapshotResourcePlural,
			kind:   reflect.TypeOf(snapshotv1alpha1.VolumeSnapshot{}).Name(),
			scope:  apiextensionsv1beta1.NamespaceScoped,
		},
		{
			name:   snapshotv1alpha1.VolumeSnapshotDataResourcePlural + "." + snapshotv1alpha1.GroupName,
			plural: snapshotv1alpha1.VolumeSnapshotDataResourcePlural,
			kind:   reflect.TypeOf(snapshotv1alpha1.VolumeSnapshotData{}).Name(),
			scope:  apiextensionsv1beta1.ClusterScoped,
		},
		{
			name:   snapshotv1alpha1.SnapshotClassResourcePlural + "." + snapshotv1alpha1.GroupName,
			plural: snapshotv1alpha1.SnapshotClassResourcePlural,
			kind:   reflect.TypeOf(snapshotv1alpha1.SnapshotClass{}).Name(),
			scope:  apiextensionsv1beta1.ClusterScoped,
		},
	}

	for _, crd := range crds {
		// initialize CRD resource if it does not exist
		err := CreateCRD(clientset, crd.name, crd.plural, crd.kind, crd.scope)
		if err != nil {
			return err
		}
	}

	for _, crd := range crds {
		// wait until CRD gets processed
		err := WaitForSnapshotResource(clientset, crd.plural)
		if err != nil {
			return err
		}
	}

	return nil
}

// CreateCRD creates CustomResourceDefinition
func CreateCRD(clientset apiextensionsclient.Interface, name, plural, kind string, scope apiextensionsv1beta1.ResourceScope) error {
	crd := &apiextensionsv1beta1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: apiextensionsv1beta1.CustomResourceDefinitionSpec{
			Group:   snapshotv1alpha1.GroupName,
			Version: snapshotv1alpha1.SchemeGroupVersion.Version,
			Scope:   scope,
			Names: apiextensionsv1beta1.CustomResourceDefinitionNames{
				Plural: plural,
				Kind:   kind,
			},
		},
	}

	_, err := clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Create(crd)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		glog.Errorf("failed to create crd: %s, err: %#v", kind, err)
		return err
	}

	return nil
}

// WaitForSnapshotResource waits for the snapshot resource
func WaitForSnapshotResource(clientset apiextensionsclient.Interface, crdName string) error {
	return wait.Poll(100*time.Millisecond, 60*time.Second, func() (bool, error) {
		crd, err := clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Get(crdName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		for _, cond := range crd.Status.Conditions {
			switch cond.Type {
			case apiextensionsv1beta1.Established:
				if cond.Status == apiextensionsv1beta1.ConditionTrue {
					return true, nil
				}
			case apiextensionsv1beta1.NamesAccepted:
				if cond.Status == apiextensionsv1beta1.ConditionFalse {
					return false, fmt.Errorf("name conflict: %v", cond.Reason)
				}
			}
		}
		return false, nil
	})
}
