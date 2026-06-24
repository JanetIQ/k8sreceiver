package janetk8sreceiver

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	conventions "go.opentelemetry.io/otel/semconv/v1.40.0"
	corev1 "k8s.io/api/core/v1"
)

const (
	K8sHierarchyScopeName string = "janetk8sreceiver/hierarchy"
	K8sEventsScopeName    string = "janetk8sreceiver/events"
	K8sEventKey           string = "k8s.event"
)

type emitter struct {
	consumer consumer.Logs
}

// baseResourceAttrs stamps the janetiq + cluster attrs that go on every record
func (e *emitter) baseResourceAttrs() pcommon.Map {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	attrs := rl.Resource().Attributes()
	return attrs
}

// EmitHierarchyNode emits a single node Add/Update/Delete as a log record
func (e *emitter) EmitHierarchyNode(ctx context.Context, node *ResolvedNode, eventType string) error {
	ld := plog.NewLogs()

	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("k8s.namespace.name", node.Namespace)
	rl.Resource().Attributes().PutStr(string(conventions.ServiceNameKey), node.Name)

	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName(K8sHierarchyScopeName)

	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	attrs := lr.Attributes()

	// Signal to downstream processors/exporters what this record is
	attrs.PutStr("janet.record.type", "k8s.hierarchy")
	attrs.PutStr("janet.event.type", eventType) // ADDED | MODIFIED | DELETED
	attrs.PutStr("janet.receiver.version", ReceiverVersion)

	// Node identity
	attrs.PutStr("k8s.object.uid", node.UID)
	attrs.PutStr("k8s.object.kind", node.Kind)
	attrs.PutStr("k8s.object.name", node.Name)
	if node.Namespace != "" {
		attrs.PutStr("k8s.object.namespace", node.Namespace)
	}

	// Full resolved ancestry attrs — k8s.pod.name, k8s.deployment.name etc
	for k, v := range node.Attrs {
		attrs.PutStr(k, v)
	}

	// build associated k8s node labels
	labels := attrs.PutEmptyMap("labels")
	for k, v := range node.Labels {
		labels.PutStr(k, v)
	}
	// build associated k8s node annotations
	ann := attrs.PutEmptyMap("annotations")
	for k, v := range node.Annotations {
		ann.PutStr(k, v)
	}

	// Edges — serialised as a slice of maps for the exporter 
	edgesSlice := attrs.PutEmptySlice("janet.edges")
	for _, edge := range node.Edges {
		edgeMap := edgesSlice.AppendEmpty().SetEmptyMap()
		edgeMap.PutStr("from_uid", edge.FromUID)
		edgeMap.PutStr("to_uid", edge.ToUID)
		edgeMap.PutStr("relation", edge.RelationName)
	}

	return e.consumer.ConsumeLogs(ctx, ld)
}

// EmitK8sEvent emits a raw k8s Event object as a log record
func (e *emitter) EmitK8sEvent(ctx context.Context, ev *corev1.Event) error {
	ld := plog.NewLogs()

	rl := ld.ResourceLogs().AppendEmpty()
	resAttrs := rl.Resource().Attributes()
	resAttrs.PutStr("k8s.namespace.name", ev.Namespace)
	resAttrs.PutStr(string(conventions.ServiceNameKey), ev.InvolvedObject.Name)

	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName(K8sEventsScopeName)

	lr := sl.LogRecords().AppendEmpty()

	ts := ev.LastTimestamp.Time
	if ts.IsZero() {
		ts = ev.EventTime.Time
	}
	if ts.IsZero() {
		ts = time.Now()
	}
	lr.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	lr.Body().SetStr(ev.Message)

	attrs := lr.Attributes()
	attrs.PutStr("janet.record.type", K8sEventKey)
	attrs.PutStr("janet.receiver.version", ReceiverVersion)
	attrs.PutStr("event.domain", "k8s")
	attrs.PutStr("event.name", ev.Name)
	attrs.PutStr("event_type", ev.Type) // Normal | Warning

	// Object the event is about
	attrs.PutStr("object_kind", ev.InvolvedObject.Kind)
	attrs.PutStr("object_name", ev.InvolvedObject.Name)
	attrs.PutStr("object_namespace", ev.InvolvedObject.Namespace)
	attrs.PutStr("object_uid", string(ev.InvolvedObject.UID))
	if ev.InvolvedObject.FieldPath != "" {
		attrs.PutStr("object_field_path", ev.InvolvedObject.FieldPath)
	}

	// Source
	attrs.PutStr("source_component", ev.Source.Component)
	if ev.Source.Host != "" {
		attrs.PutStr("source_host", ev.Source.Host)
		attrs.PutStr("k8s.node.name", ev.Source.Host)
	}

	attrs.PutInt("deprecated_count", int64(ev.Count))
	attrs.PutStr("first_timestamp", ev.FirstTimestamp.UTC().Format(time.RFC3339))
	attrs.PutStr("last_timestamp", ev.LastTimestamp.UTC().Format(time.RFC3339))

	return e.consumer.ConsumeLogs(ctx, ld)
}
