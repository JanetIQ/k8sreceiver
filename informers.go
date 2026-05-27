package janetk8sreceiver

import (
	"context"

	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
)

const (
	AddedEvent    string = "ADDED"
	ModifiedEvent string = "MODIFIED"
	DeletedEvent  string = "DELETED"
)

type objectMeta struct {
	uid       string
	name      string
	namespace string
	owners    []metav1.OwnerReference
}

type extractFn func(obj interface{}) objectMeta

func (r *janetK8sReceiver) startInformers(ctx context.Context) {
	factory := informers.NewSharedInformerFactory(r.clientset, 0)

	r.registerInformer(factory.Core().V1().Pods().Informer(), "Pod",
		func(obj interface{}) objectMeta {
			o := obj.(*corev1.Pod)
			return objectMeta{string(o.UID), o.Name, o.Namespace, o.OwnerReferences}
		},
	)

	r.registerInformer(factory.Apps().V1().ReplicaSets().Informer(), "ReplicaSet",
		func(obj interface{}) objectMeta {
			o := obj.(*appsv1.ReplicaSet)
			return objectMeta{string(o.UID), o.Name, o.Namespace, o.OwnerReferences}
		},
	)

	r.registerInformer(factory.Apps().V1().Deployments().Informer(), "Deployment",
		func(obj interface{}) objectMeta {
			o := obj.(*appsv1.Deployment)
			return objectMeta{string(o.UID), o.Name, o.Namespace, o.OwnerReferences}
		},
	)

	r.registerInformer(factory.Apps().V1().DaemonSets().Informer(), "DaemonSet",
		func(obj interface{}) objectMeta {
			o := obj.(*appsv1.DaemonSet)
			return objectMeta{string(o.UID), o.Name, o.Namespace, o.OwnerReferences}
		},
	)

	r.registerInformer(factory.Apps().V1().StatefulSets().Informer(), "StatefulSet",
		func(obj interface{}) objectMeta {
			o := obj.(*appsv1.StatefulSet)
			return objectMeta{string(o.UID), o.Name, o.Namespace, o.OwnerReferences}
		},
	)

	r.registerInformer(factory.Batch().V1().Jobs().Informer(), "Job",
		func(obj interface{}) objectMeta {
			o := obj.(*batchv1.Job)
			return objectMeta{string(o.UID), o.Name, o.Namespace, o.OwnerReferences}
		},
	)

	r.registerInformer(factory.Batch().V1().CronJobs().Informer(), "CronJob",
		func(obj interface{}) objectMeta {
			o := obj.(*batchv1.CronJob)
			return objectMeta{string(o.UID), o.Name, o.Namespace, o.OwnerReferences}
		},
	)

	r.registerInformer(factory.Core().V1().Namespaces().Informer(), "Namespace",
		func(obj interface{}) objectMeta {
			o := obj.(*corev1.Namespace)
			return objectMeta{string(o.UID), o.Name, "", nil}
		},
	)

	r.registerInformer(factory.Core().V1().Nodes().Informer(), "Node",
		func(obj interface{}) objectMeta {
			o := obj.(*corev1.Node)
			return objectMeta{string(o.UID), o.Name, "", nil}
		},
	)

	factory.Start(r.stopCh)
	factory.WaitForCacheSync(r.stopCh)
	r.logger.Info("informer cache synced")
}

func (r *janetK8sReceiver) registerInformer(
	informer cache.SharedIndexInformer,
	kind string,
	extract extractFn,
) {
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			node := r.buildNode(kind, extract(obj))
			r.index.Upsert(node)
			if err := r.emitter.EmitHierarchyNode(context.Background(), node, AddedEvent); err != nil {
				r.logger.Error("failed to emit hierarchy node", zap.Error(err))
			}
		},
		UpdateFunc: func(_, obj interface{}) {
			node := r.buildNode(kind, extract(obj))
			r.index.Upsert(node)
			if err := r.emitter.EmitHierarchyNode(context.Background(), node, ModifiedEvent); err != nil {
				r.logger.Error("failed to emit hierarchy node", zap.Error(err))
			}
		},
		DeleteFunc: func(obj interface{}) {
			meta := extract(obj)
			node := r.buildNode(kind, meta)
			r.index.Delete(meta.uid)
			if err := r.emitter.EmitHierarchyNode(context.Background(), node, DeletedEvent); err != nil {
				r.logger.Error("failed to emit hierarchy node", zap.Error(err))
			}
		},
	})
}

func (r *janetK8sReceiver) buildNode(kind string, meta objectMeta) *ResolvedNode {
	edges := make([]Edge, 0)
	for _, owner := range meta.owners {
		if owner.Controller == nil || !*owner.Controller {
			continue
		}
		rel, ok := kindToRelation[owner.Kind]
		if !ok {
			rel = "OWNED_BY"
		}
		edges = append(edges, Edge{
			FromUID:      meta.uid,
			ToUID:        string(owner.UID),
			RelationName: rel,
		})
	}

	node := &ResolvedNode{
		UID:       meta.uid,
		Kind:      kind,
		Name:      meta.name,
		Namespace: meta.namespace,
		Edges:     edges,
	}

	// Resolve full ancestry from index — parents already there after WaitForCacheSync
	node.Attrs = r.index.resolveAttrs(meta.uid, 0)

	return node
}
