package janetk8sreceiver

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	corev1 "k8s.io/api/core/v1"
)

type emitter struct {
	consumer     consumer.Logs
	datasourceID string
	clusterName  string
}

// baseResourceAttrs stamps the janetiq + cluster attrs that go on every record
func (e *emitter) baseResourceAttrs() pcommon.Map {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	attrs := rl.Resource().Attributes()
	attrs.PutStr("janetiq.datasource.id", e.datasourceID)
	attrs.PutStr("k8s.cluster.name", e.clusterName)
	return attrs
}

// EmitHierarchyNode emits a single node Add/Update/Delete as a log record
func (e *emitter) EmitHierarchyNode(ctx context.Context, node *ResolvedNode, eventType string) error {
	ld := plog.NewLogs()

	// rl := ld.ResourceLogs().AppendEmpty()
	// rl.Resource().Attributes().PutStr("janetiq.datasource.id", e.datasourceID)
	// rl.Resource().Attributes().PutStr("k8s.cluster.name", e.clusterName)

	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("janetk8sreceiver/hierarchy")

	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	attrs := lr.Attributes()

	// Signal to downstream processors/exporters what this record is
	attrs.PutStr("janetiq.record.type", "k8s.hierarchy")
	attrs.PutStr("janetiq.event.type", eventType) // ADDED | MODIFIED | DELETED

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

	// Edges — serialised as a slice of maps for the exporter to build Neo4j edges
	edgesSlice := attrs.PutEmptySlice("janetiq.edges")
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

	// rl := ld.ResourceLogs().AppendEmpty()
	// rl.Resource().Attributes().PutStr("janetiq.datasource.id", e.datasourceID)
	// rl.Resource().Attributes().PutStr("k8s.cluster.name", e.clusterName)
	// rl.Resource().Attributes().PutStr("k8s.namespace.name", ev.Namespace)

	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("janetk8sreceiver/events")

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
	attrs.PutStr("janetiq.record.type", "k8s.event")
	attrs.PutStr("event.domain", "k8s")
	attrs.PutStr("event.name", ev.Name)
	attrs.PutStr("event_type", ev.Type) // Normal | Warning
	attrs.PutStr("reason", ev.Reason)
	attrs.PutStr("note", ev.Message)

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
