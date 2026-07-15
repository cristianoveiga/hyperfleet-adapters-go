package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	privatev1alpha1 "github.com/thetechnick/orlop-gcp-hcp/api/private/v1alpha1"

	hcadapter "github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/adapters/hc"
	nodepooladapter "github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/adapters/nodepool"
	nodepoolvrresolution "github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/adapters/nodepoolvrresolution"
	placementadapter "github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/adapters/placement"
	versionresolution "github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/adapters/versionresolution"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/maestroclient"
	maestrotransport "github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/transport/maestro"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/pkg/logger"
)

// rootFlags holds values bound to the root persistent flags.
type rootFlags struct {
	logLevel  string
	logFormat string
	orlopURL  string
	workers   int
}

// maestroFlags holds Maestro-related flags shared by hc and nodepool subcommands.
type maestroFlags struct {
	grpcAddr string
	httpAddr string
	sourceID string
	clientID string
	insecure bool
}

// envOr returns the value of the environment variable named by key, or
// fallback if the variable is unset or empty.
func envOr(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func main() {
	rf := &rootFlags{}

	root := &cobra.Command{
		Use:   "hyperfleet-adapters-go",
		Short: "HyperFleet Go adapters",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			if v := envOr("LOG_LEVEL", ""); v != "" && !cmd.Flags().Changed("log-level") {
				rf.logLevel = v
			}
			if v := envOr("LOG_FORMAT", ""); v != "" && !cmd.Flags().Changed("log-format") {
				rf.logFormat = v
			}
			if v := envOr("ORLOP_URL", ""); v != "" && !cmd.Flags().Changed("orlop-url") {
				rf.orlopURL = v
			}
		},
	}

	root.PersistentFlags().StringVar(&rf.logLevel, "log-level", "info", "Log level (debug, info, warn, error)")
	root.PersistentFlags().StringVar(&rf.logFormat, "log-format", "json", "Log format (json, text)")
	root.PersistentFlags().StringVar(&rf.orlopURL, "orlop-url", "http://hyperfleet-api:8080", "Orlop API server URL for resource reads/watches [$ORLOP_URL]")
	root.PersistentFlags().IntVar(&rf.workers, "workers", 10, "Concurrent reconcile goroutines")

	root.AddCommand(
		newVersionResolutionCmd(rf),
		newNodepoolVRCmd(rf),
		newPlacementCmd(rf),
		newHCCmd(rf),
		newNodepoolCmd(rf),
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := root.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// newLogger creates a logger from root flags.
func newLogger(rf *rootFlags, component string) (logger.Logger, error) {
	return logger.NewLogger(logger.Config{
		Level:     rf.logLevel,
		Format:    rf.logFormat,
		Output:    "stdout",
		Component: component,
	})
}

// newScheme creates a runtime.Scheme with HyperFleet types registered.
func newScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	if err := privatev1alpha1.AddToScheme(scheme); err != nil {
		panic(fmt.Sprintf("failed to register HyperFleet types: %v", err))
	}
	return scheme
}

// newManager creates a controller-runtime Manager pointed at the orlop API server.
func newManager(rf *rootFlags, scheme *runtime.Scheme, log logger.Logger) (ctrl.Manager, error) {
	ctrl.SetLogger(logger.ToLogr(log))
	return ctrl.NewManager(&rest.Config{Host: rf.orlopURL}, ctrl.Options{
		Scheme:         scheme,
		LeaderElection: false,
	})
}

// controllerOpts returns per-controller options derived from root flags.
func controllerOpts(rf *rootFlags) controller.Options {
	return controller.Options{MaxConcurrentReconciles: rf.workers}
}

// ─── version-resolution ──────────────────────────────────────────────────────

func newVersionResolutionCmd(rf *rootFlags) *cobra.Command {
	var cincinnatiURL, arch string

	cmd := &cobra.Command{
		Use:   "version-resolution",
		Short: "Run the version-resolution adapter",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			log, err := newLogger(rf, "version-resolution-adapter")
			if err != nil {
				return fmt.Errorf("create logger: %w", err)
			}

			scheme := newScheme()
			mgr, err := newManager(rf, scheme, log)
			if err != nil {
				return fmt.Errorf("create manager: %w", err)
			}

			cinClient := versionresolution.NewCincinnatiClient(cincinnatiURL, arch)
			rec := versionresolution.NewReconciler(cinClient, log, mgr.GetClient())

			if err := ctrl.NewControllerManagedBy(mgr).
				For(&privatev1alpha1.Cluster{}).
				WithOptions(controllerOpts(rf)).
				Complete(rec); err != nil {
				return fmt.Errorf("setup controller: %w", err)
			}

			return mgr.Start(ctx)
		},
	}

	cmd.Flags().StringVar(&cincinnatiURL, "cincinnati-url", "https://api.openshift.com/api/upgrades_info/v1/graph", "Cincinnati API URL")
	cmd.Flags().StringVar(&arch, "arch", "amd64", "CPU architecture for Cincinnati query")

	return cmd
}

// ─── nodepool-vr ─────────────────────────────────────────────────────────────

func newNodepoolVRCmd(rf *rootFlags) *cobra.Command {
	var cincinnatiURL, arch string

	cmd := &cobra.Command{
		Use:   "nodepool-vr",
		Short: "Run the nodepool version-resolution adapter",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			log, err := newLogger(rf, "nodepool-vr-adapter")
			if err != nil {
				return fmt.Errorf("create logger: %w", err)
			}

			scheme := newScheme()
			mgr, err := newManager(rf, scheme, log)
			if err != nil {
				return fmt.Errorf("create manager: %w", err)
			}

			cinClient := versionresolution.NewCincinnatiClient(cincinnatiURL, arch)
			rec := nodepoolvrresolution.NewReconciler(cinClient, log, mgr.GetClient())

			if err := ctrl.NewControllerManagedBy(mgr).
				For(&privatev1alpha1.NodePool{}).
				WithOptions(controllerOpts(rf)).
				Complete(rec); err != nil {
				return fmt.Errorf("setup controller: %w", err)
			}

			return mgr.Start(ctx)
		},
	}

	cmd.Flags().StringVar(&cincinnatiURL, "cincinnati-url", "https://api.openshift.com/api/upgrades_info/v1/graph", "Cincinnati API URL")
	cmd.Flags().StringVar(&arch, "arch", "amd64", "CPU architecture for Cincinnati query")

	return cmd
}

// ─── placement ───────────────────────────────────────────────────────────────

func newPlacementCmd(rf *rootFlags) *cobra.Command {
	var candidateNames, baseDomains []string
	var smProject, maestroHTTPAddr string

	cmd := &cobra.Command{
		Use:   "placement",
		Short: "Run the placement adapter",
		RunE: func(cmd *cobra.Command, args []string) error {
			if v := envOr("SECRETMANAGER_PROJECT", ""); v != "" && !cmd.Flags().Changed("secretmanager-project") {
				smProject = v
			}
			if v := envOr("MAESTRO_HTTP_ADDR", ""); v != "" && !cmd.Flags().Changed("maestro-http-addr") {
				maestroHTTPAddr = v
			}

			ctx := cmd.Context()

			log, err := newLogger(rf, "placement-adapter")
			if err != nil {
				return fmt.Errorf("create logger: %w", err)
			}

			var selector placementadapter.Selector
			var candidates []placementadapter.Candidate

			if smProject != "" {
				smClient, err := secretmanager.NewClient(ctx)
				if err != nil {
					return fmt.Errorf("create secret manager client: %w", err)
				}
				defer smClient.Close() //nolint:errcheck
				selector = placementadapter.NewDynamicSelector(smClient, smProject, maestroHTTPAddr)
			} else {
				candidates = make([]placementadapter.Candidate, 0, len(candidateNames))
				for i, name := range candidateNames {
					c := placementadapter.Candidate{Name: name}
					if i < len(baseDomains) {
						c.BaseDomains = []string{baseDomains[i]}
					}
					candidates = append(candidates, c)
				}
				selector = placementadapter.NewRoundRobinSelector()
			}

			scheme := newScheme()
			mgr, err := newManager(rf, scheme, log)
			if err != nil {
				return fmt.Errorf("create manager: %w", err)
			}

			rec := placementadapter.NewReconciler(selector, candidates, log, mgr.GetClient())

			if err := ctrl.NewControllerManagedBy(mgr).
				For(&privatev1alpha1.Cluster{}).
				WithOptions(controllerOpts(rf)).
				Complete(rec); err != nil {
				return fmt.Errorf("setup controller: %w", err)
			}

			return mgr.Start(ctx)
		},
	}

	cmd.Flags().StringSliceVar(&candidateNames, "candidates", nil, "MC names (comma-separated); ignored when --secretmanager-project is set")
	cmd.Flags().StringSliceVar(&baseDomains, "base-domains", nil, "Base domains per MC, paired with --candidates")
	cmd.Flags().StringVar(&smProject, "secretmanager-project", "", "GCP project for Secret Manager MC/DNS discovery [$SECRETMANAGER_PROJECT]; enables dynamic selector")
	cmd.Flags().StringVar(&maestroHTTPAddr, "maestro-http-addr", "http://maestro.hyperfleet.svc.cluster.local:8000", "Maestro HTTP API URL for consumer discovery [$MAESTRO_HTTP_ADDR]")

	return cmd
}

// ─── hc ──────────────────────────────────────────────────────────────────────

func newHCCmd(rf *rootFlags) *cobra.Command {
	mf := &maestroFlags{}

	cmd := &cobra.Command{
		Use:   "hc",
		Short: "Run the hosted-cluster (hc) adapter",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			log, err := newLogger(rf, "hc-adapter")
			if err != nil {
				return fmt.Errorf("create logger: %w", err)
			}

			mwc, err := maestroclient.NewMaestroClient(ctx, &maestroclient.Config{
				MaestroServerAddr: mf.httpAddr,
				GRPCServerAddr:    mf.grpcAddr,
				SourceID:          mf.sourceID,
				Insecure:          mf.insecure,
			}, log)
			if err != nil {
				return fmt.Errorf("create maestro client: %w", err)
			}
			defer mwc.Close() //nolint:errcheck

			transport := maestrotransport.New(mwc, mf.sourceID, log)

			scheme := newScheme()
			mgr, err := newManager(rf, scheme, log)
			if err != nil {
				return fmt.Errorf("create manager: %w", err)
			}

			rec := hcadapter.New(transport, log, mgr.GetClient())

			if err := ctrl.NewControllerManagedBy(mgr).
				For(&privatev1alpha1.Cluster{}).
				WithOptions(controllerOpts(rf)).
				Complete(rec); err != nil {
				return fmt.Errorf("setup controller: %w", err)
			}

			return mgr.Start(ctx)
		},
	}

	cmd.Flags().StringVar(&mf.grpcAddr, "maestro-grpc-addr", "maestro-grpc.hyperfleet.svc.cluster.local:8090", "Maestro gRPC server address")
	cmd.Flags().StringVar(&mf.httpAddr, "maestro-http-addr", "http://maestro.hyperfleet.svc.cluster.local:8000", "Maestro HTTP API server address")
	cmd.Flags().StringVar(&mf.sourceID, "maestro-source-id", "hc-adapter", "Maestro source ID")
	cmd.Flags().StringVar(&mf.clientID, "maestro-client-id", "hc-adapter-client", "Maestro client ID")
	cmd.Flags().BoolVar(&mf.insecure, "maestro-insecure", true, "Disable TLS verification for Maestro connections")

	return cmd
}

// ─── nodepool ────────────────────────────────────────────────────────────────

func newNodepoolCmd(rf *rootFlags) *cobra.Command {
	mf := &maestroFlags{}

	cmd := &cobra.Command{
		Use:   "nodepool",
		Short: "Run the nodepool adapter",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			log, err := newLogger(rf, "nodepool-adapter")
			if err != nil {
				return fmt.Errorf("create logger: %w", err)
			}

			mwc, err := maestroclient.NewMaestroClient(ctx, &maestroclient.Config{
				MaestroServerAddr: mf.httpAddr,
				GRPCServerAddr:    mf.grpcAddr,
				SourceID:          mf.sourceID,
				Insecure:          mf.insecure,
			}, log)
			if err != nil {
				return fmt.Errorf("create maestro client: %w", err)
			}
			defer mwc.Close() //nolint:errcheck

			transport := maestrotransport.New(mwc, mf.sourceID, log)

			scheme := newScheme()
			mgr, err := newManager(rf, scheme, log)
			if err != nil {
				return fmt.Errorf("create manager: %w", err)
			}

			rec := nodepooladapter.New(transport, log, mgr.GetClient())

			if err := ctrl.NewControllerManagedBy(mgr).
				For(&privatev1alpha1.NodePool{}).
				WithOptions(controllerOpts(rf)).
				Complete(rec); err != nil {
				return fmt.Errorf("setup controller: %w", err)
			}

			return mgr.Start(ctx)
		},
	}

	cmd.Flags().StringVar(&mf.grpcAddr, "maestro-grpc-addr", "maestro-grpc.hyperfleet.svc.cluster.local:8090", "Maestro gRPC server address")
	cmd.Flags().StringVar(&mf.httpAddr, "maestro-http-addr", "http://maestro.hyperfleet.svc.cluster.local:8000", "Maestro HTTP API server address")
	cmd.Flags().StringVar(&mf.sourceID, "maestro-source-id", "nodepool-adapter", "Maestro source ID")
	cmd.Flags().StringVar(&mf.clientID, "maestro-client-id", "nodepool-adapter-client", "Maestro client ID")
	cmd.Flags().BoolVar(&mf.insecure, "maestro-insecure", true, "Disable TLS verification for Maestro connections")

	return cmd
}

