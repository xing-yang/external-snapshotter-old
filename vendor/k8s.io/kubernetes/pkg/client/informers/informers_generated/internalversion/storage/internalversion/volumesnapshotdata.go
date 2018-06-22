/*
Copyright The Kubernetes Authors.

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

// Code generated by informer-gen. DO NOT EDIT.

package internalversion

import (
	time "time"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	watch "k8s.io/apimachinery/pkg/watch"
	cache "k8s.io/client-go/tools/cache"
	storage "k8s.io/kubernetes/pkg/apis/storage"
	internalclientset "k8s.io/kubernetes/pkg/client/clientset_generated/internalclientset"
	internalinterfaces "k8s.io/kubernetes/pkg/client/informers/informers_generated/internalversion/internalinterfaces"
	internalversion "k8s.io/kubernetes/pkg/client/listers/storage/internalversion"
)

// VolumeSnapshotDataInformer provides access to a shared informer and lister for
// VolumeSnapshotDatas.
type VolumeSnapshotDataInformer interface {
	Informer() cache.SharedIndexInformer
	Lister() internalversion.VolumeSnapshotDataLister
}

type volumeSnapshotDataInformer struct {
	factory          internalinterfaces.SharedInformerFactory
	tweakListOptions internalinterfaces.TweakListOptionsFunc
}

// NewVolumeSnapshotDataInformer constructs a new informer for VolumeSnapshotData type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewVolumeSnapshotDataInformer(client internalclientset.Interface, resyncPeriod time.Duration, indexers cache.Indexers) cache.SharedIndexInformer {
	return NewFilteredVolumeSnapshotDataInformer(client, resyncPeriod, indexers, nil)
}

// NewFilteredVolumeSnapshotDataInformer constructs a new informer for VolumeSnapshotData type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewFilteredVolumeSnapshotDataInformer(client internalclientset.Interface, resyncPeriod time.Duration, indexers cache.Indexers, tweakListOptions internalinterfaces.TweakListOptionsFunc) cache.SharedIndexInformer {
	return cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options v1.ListOptions) (runtime.Object, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.Storage().VolumeSnapshotDatas().List(options)
			},
			WatchFunc: func(options v1.ListOptions) (watch.Interface, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.Storage().VolumeSnapshotDatas().Watch(options)
			},
		},
		&storage.VolumeSnapshotData{},
		resyncPeriod,
		indexers,
	)
}

func (f *volumeSnapshotDataInformer) defaultInformer(client internalclientset.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	return NewFilteredVolumeSnapshotDataInformer(client, resyncPeriod, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}, f.tweakListOptions)
}

func (f *volumeSnapshotDataInformer) Informer() cache.SharedIndexInformer {
	return f.factory.InformerFor(&storage.VolumeSnapshotData{}, f.defaultInformer)
}

func (f *volumeSnapshotDataInformer) Lister() internalversion.VolumeSnapshotDataLister {
	return internalversion.NewVolumeSnapshotDataLister(f.Informer().GetIndexer())
}