package placement

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── mock secretLookup ────────────────────────────────────────────────────────

// mockSMLookup implements secretLookup for tests.
// listResponses maps filter string to the secrets returned for that filter.
// A single listErr short-circuits all listSecrets calls.
type mockSMLookup struct {
	listResponses map[string][]*secretmanagerpb.Secret
	listErr       error
	secretData    []byte
	accessErr     error
	accessedName  string // last name passed to accessSecretVersion
}

func (m *mockSMLookup) listSecrets(_ context.Context, _, filter string) ([]*secretmanagerpb.Secret, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	return m.listResponses[filter], nil
}

func (m *mockSMLookup) accessSecretVersion(_ context.Context, name string) ([]byte, error) {
	m.accessedName = name
	if m.accessErr != nil {
		return nil, m.accessErr
	}
	return m.secretData, nil
}

// ─── helpers ──────────────────────────────────────────────────────────────────

const (
	filterMCNames  = "labels.maestro-consumer-name:*"
	filterArgoCD   = `labels.infra-type:region name:argocd-cluster`
	testProject    = "my-project"
)

// smSecret builds a minimal secretmanagerpb.Secret with the given name and labels.
func smSecret(name string, labels map[string]string) *secretmanagerpb.Secret {
	return &secretmanagerpb.Secret{Name: name, Labels: labels}
}

// maestroResponse encodes a Maestro consumers API response body.
func maestroResponse(names ...string) []byte {
	type item struct {
		Name string `json:"name"`
	}
	type page struct {
		Items []item `json:"items"`
	}
	items := make([]item, 0, len(names))
	for _, n := range names {
		items = append(items, item{Name: n})
	}
	b, _ := json.Marshal(page{Items: items})
	return b
}

// dnsPayload encodes a Secret Manager payload with the given comma-separated domains.
func dnsPayload(domains string) []byte {
	b, _ := json.Marshal(map[string]string{"meta_hc_dns_domains": domains})
	return b
}

// newSelector constructs a DynamicSelector using the given mock SM lookup and
// an HTTP client pointed at the provided test server (may be nil).
func newSelector(sm secretLookup, srv *httptest.Server) *DynamicSelector {
	maestroURL := ""
	httpClient := &http.Client{}
	if srv != nil {
		maestroURL = srv.URL
		httpClient = srv.Client()
	}
	return &DynamicSelector{
		smLookup:   sm,
		project:    testProject,
		maestroURL: maestroURL,
		httpClient: httpClient,
	}
}

// maestroServer returns a test HTTP server that handles the consumers endpoint.
// If statusCode != 200, it returns that code with the given body.
// Otherwise it returns 200 with body.
func maestroServer(t *testing.T, statusCode int, body []byte) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/maestro/v1/consumers", r.URL.Path)
		w.WriteHeader(statusCode)
		_, _ = w.Write(body)
	}))
}

// ─── maestroConsumerNames ─────────────────────────────────────────────────────

func TestDynamicSelector_maestroConsumerNames(t *testing.T) {
	ctx := context.Background()

	t.Run("returns consumer names from valid response", func(t *testing.T) {
		srv := maestroServer(t, http.StatusOK, maestroResponse("mc-a", "mc-b", "mc-c"))
		defer srv.Close()

		s := newSelector(&mockSMLookup{}, srv)
		names, err := s.maestroConsumerNames(ctx)
		require.NoError(t, err)
		assert.Equal(t, []string{"mc-a", "mc-b", "mc-c"}, names)
	})

	t.Run("filters out empty consumer names", func(t *testing.T) {
		body := []byte(`{"items":[{"name":"mc-a"},{"name":""},{"name":"mc-b"}]}`)
		srv := maestroServer(t, http.StatusOK, body)
		defer srv.Close()

		s := newSelector(&mockSMLookup{}, srv)
		names, err := s.maestroConsumerNames(ctx)
		require.NoError(t, err)
		assert.Equal(t, []string{"mc-a", "mc-b"}, names)
	})

	t.Run("empty items array → empty slice, no error", func(t *testing.T) {
		srv := maestroServer(t, http.StatusOK, maestroResponse())
		defer srv.Close()

		s := newSelector(&mockSMLookup{}, srv)
		names, err := s.maestroConsumerNames(ctx)
		require.NoError(t, err)
		assert.Empty(t, names)
	})

	t.Run("non-200 status → error containing status code", func(t *testing.T) {
		srv := maestroServer(t, http.StatusInternalServerError, []byte("oops"))
		defer srv.Close()

		s := newSelector(&mockSMLookup{}, srv)
		_, err := s.maestroConsumerNames(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "500")
	})

	t.Run("invalid JSON body → error", func(t *testing.T) {
		srv := maestroServer(t, http.StatusOK, []byte("not-json"))
		defer srv.Close()

		s := newSelector(&mockSMLookup{}, srv)
		_, err := s.maestroConsumerNames(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("unreachable server → HTTP error", func(t *testing.T) {
		s := newSelector(&mockSMLookup{}, nil)
		s.maestroURL = "http://127.0.0.1:0" // nothing listening
		_, err := s.maestroConsumerNames(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "GET consumers")
	})
}

// ─── smMCNames ────────────────────────────────────────────────────────────────

func TestDynamicSelector_smMCNames(t *testing.T) {
	ctx := context.Background()

	t.Run("returns MC names from label values", func(t *testing.T) {
		sm := &mockSMLookup{
			listResponses: map[string][]*secretmanagerpb.Secret{
				filterMCNames: {
					smSecret("projects/p/secrets/s1", map[string]string{"maestro-consumer-name": "mc-a"}),
					smSecret("projects/p/secrets/s2", map[string]string{"maestro-consumer-name": "mc-b"}),
				},
			},
		}
		s := newSelector(sm, nil)
		names, err := s.smMCNames(ctx)
		require.NoError(t, err)
		assert.Equal(t, []string{"mc-a", "mc-b"}, names)
	})

	t.Run("filters secrets with empty label value", func(t *testing.T) {
		sm := &mockSMLookup{
			listResponses: map[string][]*secretmanagerpb.Secret{
				filterMCNames: {
					smSecret("projects/p/secrets/s1", map[string]string{"maestro-consumer-name": "mc-a"}),
					smSecret("projects/p/secrets/s2", map[string]string{"maestro-consumer-name": ""}),
				},
			},
		}
		s := newSelector(sm, nil)
		names, err := s.smMCNames(ctx)
		require.NoError(t, err)
		assert.Equal(t, []string{"mc-a"}, names)
	})

	t.Run("no secrets → empty slice, no error", func(t *testing.T) {
		sm := &mockSMLookup{listResponses: map[string][]*secretmanagerpb.Secret{}}
		s := newSelector(sm, nil)
		names, err := s.smMCNames(ctx)
		require.NoError(t, err)
		assert.Empty(t, names)
	})

	t.Run("listSecrets error → wrapped error", func(t *testing.T) {
		sm := &mockSMLookup{listErr: fmt.Errorf("gcp unavailable")}
		s := newSelector(sm, nil)
		_, err := s.smMCNames(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "list secrets")
		assert.Contains(t, err.Error(), "gcp unavailable")
	})
}

// ─── hcDNSDomains ─────────────────────────────────────────────────────────────

func TestDynamicSelector_hcDNSDomains(t *testing.T) {
	ctx := context.Background()

	t.Run("parses single domain from secret payload", func(t *testing.T) {
		sm := &mockSMLookup{
			listResponses: map[string][]*secretmanagerpb.Secret{
				filterArgoCD: {smSecret("projects/p/secrets/argocd", nil)},
			},
			secretData: dnsPayload("us-central1.example.com"),
		}
		s := newSelector(sm, nil)
		domains, err := s.hcDNSDomains(ctx)
		require.NoError(t, err)
		assert.Equal(t, []string{"us-central1.example.com"}, domains)
	})

	t.Run("splits comma-separated domains", func(t *testing.T) {
		sm := &mockSMLookup{
			listResponses: map[string][]*secretmanagerpb.Secret{
				filterArgoCD: {smSecret("projects/p/secrets/argocd", nil)},
			},
			secretData: dnsPayload("a.example.com,b.example.com,c.example.com"),
		}
		s := newSelector(sm, nil)
		domains, err := s.hcDNSDomains(ctx)
		require.NoError(t, err)
		assert.Equal(t, []string{"a.example.com", "b.example.com", "c.example.com"}, domains)
	})

	t.Run("trims whitespace around domain entries", func(t *testing.T) {
		sm := &mockSMLookup{
			listResponses: map[string][]*secretmanagerpb.Secret{
				filterArgoCD: {smSecret("projects/p/secrets/argocd", nil)},
			},
			secretData: dnsPayload("  a.example.com , b.example.com "),
		}
		s := newSelector(sm, nil)
		domains, err := s.hcDNSDomains(ctx)
		require.NoError(t, err)
		assert.Equal(t, []string{"a.example.com", "b.example.com"}, domains)
	})

	t.Run("uses first matching secret when multiple returned", func(t *testing.T) {
		sm := &mockSMLookup{
			listResponses: map[string][]*secretmanagerpb.Secret{
				filterArgoCD: {
					smSecret("projects/p/secrets/first", nil),
					smSecret("projects/p/secrets/second", nil),
				},
			},
			secretData: dnsPayload("first.example.com"),
		}
		s := newSelector(sm, nil)
		_, err := s.hcDNSDomains(ctx)
		require.NoError(t, err)
		assert.Equal(t, "projects/p/secrets/first/versions/latest", sm.accessedName)
	})

	t.Run("no matching secret → error mentioning project", func(t *testing.T) {
		sm := &mockSMLookup{listResponses: map[string][]*secretmanagerpb.Secret{}}
		s := newSelector(sm, nil)
		_, err := s.hcDNSDomains(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), testProject)
	})

	t.Run("listSecrets error → error propagated", func(t *testing.T) {
		sm := &mockSMLookup{listErr: fmt.Errorf("gcp down")}
		s := newSelector(sm, nil)
		_, err := s.hcDNSDomains(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "list argocd-cluster secrets")
	})

	t.Run("accessSecretVersion error → error propagated", func(t *testing.T) {
		sm := &mockSMLookup{
			listResponses: map[string][]*secretmanagerpb.Secret{
				filterArgoCD: {smSecret("projects/p/secrets/argocd", nil)},
			},
			accessErr: fmt.Errorf("permission denied"),
		}
		s := newSelector(sm, nil)
		_, err := s.hcDNSDomains(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "access secret")
		assert.Contains(t, err.Error(), "permission denied")
	})

	t.Run("invalid JSON secret payload → error", func(t *testing.T) {
		sm := &mockSMLookup{
			listResponses: map[string][]*secretmanagerpb.Secret{
				filterArgoCD: {smSecret("projects/p/secrets/argocd", nil)},
			},
			secretData: []byte("not-json"),
		}
		s := newSelector(sm, nil)
		_, err := s.hcDNSDomains(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("empty meta_hc_dns_domains field → empty slice", func(t *testing.T) {
		sm := &mockSMLookup{
			listResponses: map[string][]*secretmanagerpb.Secret{
				filterArgoCD: {smSecret("projects/p/secrets/argocd", nil)},
			},
			secretData: dnsPayload(""),
		}
		s := newSelector(sm, nil)
		domains, err := s.hcDNSDomains(ctx)
		require.NoError(t, err)
		assert.Empty(t, domains)
	})
}

// ─── eligibleMCs ──────────────────────────────────────────────────────────────

func TestDynamicSelector_eligibleMCs(t *testing.T) {
	ctx := context.Background()

	makeSM := func(mcNames ...string) *mockSMLookup {
		secrets := make([]*secretmanagerpb.Secret, 0, len(mcNames))
		for i, n := range mcNames {
			secrets = append(secrets, smSecret(
				fmt.Sprintf("projects/p/secrets/s%d", i),
				map[string]string{"maestro-consumer-name": n},
			))
		}
		return &mockSMLookup{
			listResponses: map[string][]*secretmanagerpb.Secret{filterMCNames: secrets},
		}
	}

	t.Run("returns intersection of SM and Maestro sets", func(t *testing.T) {
		sm := makeSM("mc-a", "mc-b", "mc-c")
		srv := maestroServer(t, http.StatusOK, maestroResponse("mc-b", "mc-c", "mc-d"))
		defer srv.Close()

		s := newSelector(sm, srv)
		eligible, err := s.eligibleMCs(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"mc-b", "mc-c"}, eligible)
	})

	t.Run("SM returns empty → no eligible MCs", func(t *testing.T) {
		sm := makeSM() // no secrets
		srv := maestroServer(t, http.StatusOK, maestroResponse("mc-a"))
		defer srv.Close()

		s := newSelector(sm, srv)
		eligible, err := s.eligibleMCs(ctx)
		require.NoError(t, err)
		assert.Empty(t, eligible)
	})

	t.Run("Maestro returns empty → no eligible MCs", func(t *testing.T) {
		sm := makeSM("mc-a", "mc-b")
		srv := maestroServer(t, http.StatusOK, maestroResponse())
		defer srv.Close()

		s := newSelector(sm, srv)
		eligible, err := s.eligibleMCs(ctx)
		require.NoError(t, err)
		assert.Empty(t, eligible)
	})

	t.Run("no overlap between SM and Maestro → empty", func(t *testing.T) {
		sm := makeSM("mc-a", "mc-b")
		srv := maestroServer(t, http.StatusOK, maestroResponse("mc-c", "mc-d"))
		defer srv.Close()

		s := newSelector(sm, srv)
		eligible, err := s.eligibleMCs(ctx)
		require.NoError(t, err)
		assert.Empty(t, eligible)
	})

	t.Run("SM error → error propagated", func(t *testing.T) {
		sm := &mockSMLookup{listErr: fmt.Errorf("sm down")}
		srv := maestroServer(t, http.StatusOK, maestroResponse("mc-a"))
		defer srv.Close()

		s := newSelector(sm, srv)
		_, err := s.eligibleMCs(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "secret manager MC lookup")
	})

	t.Run("Maestro error → error propagated", func(t *testing.T) {
		sm := makeSM("mc-a")
		srv := maestroServer(t, http.StatusInternalServerError, []byte("maestro down"))
		defer srv.Close()

		s := newSelector(sm, srv)
		_, err := s.eligibleMCs(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "maestro consumer lookup")
	})
}

// ─── Select ───────────────────────────────────────────────────────────────────

func TestDynamicSelector_Select(t *testing.T) {
	ctx := context.Background()

	// buildFullMock wires up SM with the given MC names (in both SM and Maestro)
	// and the given DNS domains, then returns the selector + test server.
	buildFullMock := func(t *testing.T, mcNames []string, domains string) (*DynamicSelector, *httptest.Server) {
		t.Helper()
		secrets := make([]*secretmanagerpb.Secret, 0, len(mcNames))
		for i, n := range mcNames {
			secrets = append(secrets, smSecret(
				fmt.Sprintf("projects/p/secrets/mc%d", i),
				map[string]string{"maestro-consumer-name": n},
			))
		}
		sm := &mockSMLookup{
			listResponses: map[string][]*secretmanagerpb.Secret{
				filterMCNames: secrets,
				filterArgoCD:  {smSecret("projects/p/secrets/argocd", nil)},
			},
			secretData: dnsPayload(domains),
		}
		srv := maestroServer(t, http.StatusOK, maestroResponse(mcNames...))
		return newSelector(sm, srv), srv
	}

	t.Run("happy path: returns single MC and domain", func(t *testing.T) {
		s, srv := buildFullMock(t, []string{"mc-a"}, "us-central1.example.com")
		defer srv.Close()

		mc, domain, err := s.Select(ctx, nil)
		require.NoError(t, err)
		assert.Equal(t, "mc-a", mc)
		assert.Equal(t, "us-central1.example.com", domain)
	})

	t.Run("round-robins across multiple eligible MCs", func(t *testing.T) {
		s, srv := buildFullMock(t, []string{"mc-a", "mc-b", "mc-c"}, "example.com")
		defer srv.Close()

		seen := map[string]bool{}
		for i := 0; i < 6; i++ {
			mc, _, err := s.Select(ctx, nil)
			require.NoError(t, err)
			seen[mc] = true
		}
		assert.True(t, seen["mc-a"], "mc-a should be selected at least once")
		assert.True(t, seen["mc-b"], "mc-b should be selected at least once")
		assert.True(t, seen["mc-c"], "mc-c should be selected at least once")
	})

	t.Run("round-robins across multiple DNS domains independently", func(t *testing.T) {
		s, srv := buildFullMock(t, []string{"mc-a"}, "zone1.example.com,zone2.example.com")
		defer srv.Close()

		seen := map[string]bool{}
		for i := 0; i < 4; i++ {
			_, domain, err := s.Select(ctx, nil)
			require.NoError(t, err)
			seen[domain] = true
		}
		assert.True(t, seen["zone1.example.com"])
		assert.True(t, seen["zone2.example.com"])
	})

	t.Run("no eligible MCs → error mentioning check hint", func(t *testing.T) {
		// SM has mc-a but Maestro has none → intersection is empty
		sm := &mockSMLookup{
			listResponses: map[string][]*secretmanagerpb.Secret{
				filterMCNames: {smSecret("s1", map[string]string{"maestro-consumer-name": "mc-a"})},
			},
		}
		srv := maestroServer(t, http.StatusOK, maestroResponse()) // no consumers
		defer srv.Close()

		s := newSelector(sm, srv)
		_, _, err := s.Select(ctx, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no eligible management clusters")
	})

	t.Run("no DNS domains in secret → error mentioning project", func(t *testing.T) {
		sm := &mockSMLookup{
			listResponses: map[string][]*secretmanagerpb.Secret{
				filterMCNames: {smSecret("s1", map[string]string{"maestro-consumer-name": "mc-a"})},
				filterArgoCD:  {smSecret("projects/p/secrets/argocd", nil)},
			},
			secretData: dnsPayload(""), // empty domains
		}
		srv := maestroServer(t, http.StatusOK, maestroResponse("mc-a"))
		defer srv.Close()

		s := newSelector(sm, srv)
		_, _, err := s.Select(ctx, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no HC DNS domains")
	})

	t.Run("SM error during eligible MC discovery → error propagated", func(t *testing.T) {
		sm := &mockSMLookup{listErr: fmt.Errorf("gcp outage")}
		srv := maestroServer(t, http.StatusOK, maestroResponse("mc-a"))
		defer srv.Close()

		s := newSelector(sm, srv)
		_, _, err := s.Select(ctx, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "discover eligible MCs")
	})

	t.Run("Maestro error → error propagated through Select", func(t *testing.T) {
		sm := &mockSMLookup{
			listResponses: map[string][]*secretmanagerpb.Secret{
				filterMCNames: {smSecret("s1", map[string]string{"maestro-consumer-name": "mc-a"})},
			},
		}
		srv := maestroServer(t, http.StatusServiceUnavailable, []byte("down"))
		defer srv.Close()

		s := newSelector(sm, srv)
		_, _, err := s.Select(ctx, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "discover eligible MCs")
	})
}