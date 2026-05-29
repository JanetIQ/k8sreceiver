package janetk8sreceiver

import (
	"sync"
)

type ResolvedNode struct {
	UID       string
	Kind      string
	Name      string
	Namespace string
	// Full ancestry attrs — ready to stamp onto log records
	// e.g. "k8s.pod.name" -> "bad-pod", "k8s.deployment.name" -> "bad-pod"
	Attrs       map[string]string
	Labels      map[string]string
	Annotations map[string]string
	// Direct edges to parent UIDs
	Edges []Edge
}

type Edge struct {
	FromUID      string
	ToUID        string
	RelationName string
}

type HierarchyIndex struct {
	mu    sync.RWMutex
	byUID map[string]*ResolvedNode
	// "Kind/namespace/name" -> uid — for event enrichment lookups
	byKey map[string]string
}

func newHierarchyIndex() *HierarchyIndex {
	return &HierarchyIndex{
		byUID: make(map[string]*ResolvedNode),
		byKey: make(map[string]string),
	}
}

func (h *HierarchyIndex) Upsert(node *ResolvedNode) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.byUID[node.UID] = node
	h.byKey[indexKey(node.Kind, node.Namespace, node.Name)] = node.UID
}

func (h *HierarchyIndex) Delete(uid string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if node, ok := h.byUID[uid]; ok {
		delete(h.byKey, indexKey(node.Kind, node.Namespace, node.Name))
		delete(h.byUID, uid)
	}
}

func (h *HierarchyIndex) GetByUID(uid string) (*ResolvedNode, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	n, ok := h.byUID[uid]
	return n, ok
}

func (h *HierarchyIndex) GetByKey(kind, namespace, name string) (*ResolvedNode, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	uid, ok := h.byKey[indexKey(kind, namespace, name)]
	if !ok {
		return nil, false
	}
	n, ok := h.byUID[uid]
	return n, ok
}

// resolveAttrs walks ownerReferences upward and builds the full attr map.
// Called under no lock — callers must ensure parents are in index first.
func (h *HierarchyIndex) resolveAttrs(uid string, depth int) map[string]string {
	if depth > 10 {
		return nil
	}

	h.mu.RLock()
	node, ok := h.byUID[uid]
	h.mu.RUnlock()
	if !ok {
		return nil
	}

	attrs := make(map[string]string)

	if otelKey, ok := kindToOtelKey[node.Kind]; ok {
		attrs[otelKey] = node.Name
	}
	if node.Namespace != "" {
		attrs["k8s.namespace.name"] = node.Namespace
	}

	for _, edge := range node.Edges {
		for k, v := range h.resolveAttrs(edge.ToUID, depth+1) {
			attrs[k] = v
		}
	}

	return attrs
}

func indexKey(kind, namespace, name string) string {
	return kind + "/" + namespace + "/" + name
}

// kindToOtelKey maps K8s object kinds to OTel semconv attribute keys
var kindToOtelKey = map[string]string{
	"Pod":                     "k8s.pod.name",
	"ReplicaSet":              "k8s.replicaset.name",
	"Deployment":              "k8s.deployment.name",
	"StatefulSet":             "k8s.statefulset.name",
	"DaemonSet":               "k8s.daemonset.name",
	"Job":                     "k8s.job.name",
	"CronJob":                 "k8s.cronjob.name",
	"Namespace":               "k8s.namespace.name",
	"Node":                    "k8s.node.name",
	"Service":                 "k8s.service.name",
	"ReplicationController":   "k8s.replicationcontroller.name",
	"HorizontalPodAutoscaler": "k8s.hpa.name",
}

// kindToRelation maps K8s owner kinds to edge relation names
var kindToRelation = map[string]string{
	"ReplicaSet":  "MANAGES",
	"Deployment":  "CONTAINS",
	"DaemonSet":   "MANAGES",
	"StatefulSet": "MANAGES",
	"Job":         "MANAGES",
	"CronJob":     "CONTAINS",
	"Node":        "CONTAINS",
	"Namespace":   "CONTAINS",
	"Cluster":     "OWNS",
}
