package janetk8sreceiver

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type janetK8sReceiver struct {
	config      *Config
	logger      *zap.Logger
	clientset   kubernetes.Interface
	clusterName string
	clusterUID  string
	index       *HierarchyIndex
	emitter     *emitter
	stopCh      chan struct{}
}

type clusterIdentity struct {
	Name string
	UID  string // for now this will be the "k8s_cluster_"+ $(kube-system namespace UID)
}

func newReceiver(cfg *Config, logger *zap.Logger, consumer consumer.Logs) (*janetK8sReceiver, error) {
	return &janetK8sReceiver{
		config:  cfg,
		logger:  logger,
		index:   newHierarchyIndex(),
		emitter: &emitter{consumer: consumer},
		stopCh:  make(chan struct{}),
	}, nil
}

func (r *janetK8sReceiver) inferClusterIdentity(ctx context.Context, clientset kubernetes.Interface, kubeconfigPath string) (clusterIdentity, error) {

	ns, err := clientset.CoreV1().Namespaces().Get(ctx, "kube-system", metav1.GetOptions{})
	if err != nil {
		return clusterIdentity{}, err
	}
	uid := "k8s_cluster_" + string(ns.UID)

	name := ""
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfigPath != "" {
		loadingRules.ExplicitPath = kubeconfigPath
	}
	if cfg, err := loadingRules.Load(); err == nil && cfg.CurrentContext != "" {
		name = cfg.CurrentContext
	}

	return clusterIdentity{Name: name, UID: uid}, nil
}

func (r *janetK8sReceiver) Start(ctx context.Context, host component.Host) error {
	k8sConfig, err := r.buildK8sConfig()
	if err != nil {
		return fmt.Errorf("building k8s config: %w", err)
	}

	r.clientset, err = kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		return fmt.Errorf("creating k8s client: %w", err)
	}

	identity, err := r.inferClusterIdentity(ctx, r.clientset, r.config.KubeconfigPath)
	if err != nil {
		return fmt.Errorf("Failed to infer cluster identity: %w", err)
	}
	r.clusterName = identity.Name
	r.clusterUID = identity.UID

	// create cluster node
	node := r.buildClusterNode()
	r.index.Upsert(node)
	if err := r.emitter.EmitHierarchyNode(context.Background(), node, AddedEvent); err != nil {
		return fmt.Errorf("Failed to create cluster node: %w", err)
	}

	// Start informers — blocks until cache is synced then returns
	r.startInformers(ctx)

	// Start event watcher in background
	go r.watchEvents(ctx)

	return nil
}

func (r *janetK8sReceiver) Shutdown(ctx context.Context) error {
	close(r.stopCh)
	return nil
}

func (r *janetK8sReceiver) buildK8sConfig() (*rest.Config, error) {
	if r.config.KubeconfigPath != "" {
		return clientcmd.BuildConfigFromFlags("", r.config.KubeconfigPath)
	}
	// In-cluster config — used when receiver runs inside the cluster
	return rest.InClusterConfig()
}

// watchEvents watches cluster-wide k8s events with reconnect logic
func (r *janetK8sReceiver) watchEvents(ctx context.Context) {
	for {
		select {
		case <-r.stopCh:
			return
		default:
		}

		if err := r.runEventWatch(ctx); err != nil {
			r.logger.Error("event watch error, reconnecting", zap.Error(err))
			select {
			case <-r.stopCh:
				return
			case <-time.After(5 * time.Second):
			}
		}
	}
}

func (r *janetK8sReceiver) runEventWatch(ctx context.Context) error {
	watcher, err := r.clientset.CoreV1().Events("").Watch(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("starting event watch: %w", err)
	}
	defer watcher.Stop()

	r.logger.Info("event watch started")

	for {
		select {
		case <-r.stopCh:
			return nil

		case event, ok := <-watcher.ResultChan():
			if !ok {
				return fmt.Errorf("event watch channel closed")
			}

			if event.Type == watch.Error {
				return fmt.Errorf("watch error event received")
			}

			if event.Type == watch.Bookmark {
				continue
			}

			ev, ok := event.Object.(*corev1.Event)
			if !ok {
				continue
			}

			// Enrich with hierarchy from index before emitting
			r.enrichEvent(ev)

			if err := r.emitter.EmitK8sEvent(ctx, ev); err != nil {
				r.logger.Error("failed to emit k8s event", zap.Error(err))
			}
		}
	}
}

// enrichEvent stamps hierarchy attrs onto the event's InvolvedObject
// by looking up the object in the hierarchy index
func (r *janetK8sReceiver) enrichEvent(ev *corev1.Event) {
	node, ok := r.index.GetByKey(
		ev.InvolvedObject.Kind,
		ev.InvolvedObject.Namespace,
		ev.InvolvedObject.Name,
	)
	if !ok {
		return
	}
	// Store resolved attrs on the event for emitter to pick up
	// We attach them as annotations since corev1.Event has no extra attrs field
	// emitter reads from ev.Annotations["janet.*"]
	if ev.Annotations == nil {
		ev.Annotations = make(map[string]string)
	}
	for k, v := range node.Attrs {
		ev.Annotations["janet.resolved."+k] = v
	}
}
