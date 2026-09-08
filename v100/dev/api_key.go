package dev

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/google/uuid"
	"github.com/urfave/cli"
)

// APIKeyCollection is the Firestore collection holding API key documents.
// Each document is keyed by the API key itself and holds the user it was
// issued to (see APIKeyRecord).
const APIKeyCollection = "APIKeys"

// APIKeyRecord is stored at APIKeys/<key>, where <key> is the API key itself.
type APIKeyRecord struct {
	User string `firestore:"user"`
}

// addAPIKey generates a new random API key, records it in Firestore as
// belonging to user, and returns the generated key.
func addAPIKey(ctx context.Context, fs *firestore.Client, user string) (string, error) {
	key := uuid.New().String()
	if _, err := fs.Collection(APIKeyCollection).Doc(key).Set(ctx, APIKeyRecord{User: user}); err != nil {
		return "", fmt.Errorf("writing %s/%s to firestore: %w", APIKeyCollection, key, err)
	}
	return key, nil
}

func runAddAPIKey(c *cli.Context) error {
	user := c.Args().Get(0)
	if user == "" {
		return fmt.Errorf("user is required")
	}
	project := c.String("project")
	if project == "" {
		return fmt.Errorf("--project is required")
	}

	ctx := context.Background()

	fsClient, err := firestore.NewClientWithDatabase(ctx, project, c.String("db"))
	if err != nil {
		return fmt.Errorf("creating firestore client: %w", err)
	}
	defer fsClient.Close()

	key, err := addAPIKey(ctx, fsClient, user)
	if err != nil {
		return err
	}
	fmt.Printf("API key for %s: %s\n", user, key)

	return nil
}

// ----- auth middleware -----

type ctxKey int

const userContextKey ctxKey = iota

// userFromContext returns the user associated with the API key that
// authenticated the current request, or "" if none (e.g. the request wasn't
// under /api/ and so wasn't authenticated).
func userFromContext(ctx context.Context) string {
	user, _ := ctx.Value(userContextKey).(string)
	return user
}

// apiKeyCacheTTL is how long a successful API-key lookup is cached before
// apiKeyAuthMiddleware will hit Firestore again for that key.
const apiKeyCacheTTL = 10 * time.Minute

// apiKeyCacheEntry is a cached (user, expiry) pair for a single API key.
type apiKeyCacheEntry struct {
	user      string
	expiresAt time.Time
}

// apiKeyCache caches APIKeys Firestore lookups in memory so
// apiKeyAuthMiddleware doesn't re-fetch the same key's user on every request.
// Safe for concurrent use.
type apiKeyCache struct {
	mu      sync.Mutex
	entries map[string]apiKeyCacheEntry
}

func newAPIKeyCache() *apiKeyCache {
	return &apiKeyCache{entries: make(map[string]apiKeyCacheEntry)}
}

// lookup returns the user associated with key, reading through to Firestore
// (and populating the cache) on a miss or expired entry.
func (c *apiKeyCache) lookup(ctx context.Context, fs *firestore.Client, key string) (string, error) {
	c.mu.Lock()
	entry, ok := c.entries[key]
	c.mu.Unlock()
	if ok && time.Now().Before(entry.expiresAt) {
		return entry.user, nil
	}

	snap, err := fs.Collection(APIKeyCollection).Doc(key).Get(ctx)
	if err != nil {
		return "", err
	}
	var rec APIKeyRecord
	if err := snap.DataTo(&rec); err != nil {
		return "", err
	}

	c.mu.Lock()
	c.entries[key] = apiKeyCacheEntry{user: rec.User, expiresAt: time.Now().Add(apiKeyCacheTTL)}
	c.mu.Unlock()

	return rec.User, nil
}

// apiKeyAuthMiddleware requires every request under prefix+"/api/" to carry
// a valid "Authorization: Bearer <key>" header, where <key> is looked up in
// the APIKeys Firestore collection (cached in memory for apiKeyCacheTTL so
// Firestore isn't hit on every request). On success, the user the key was
// issued to is stashed in the request context (retrievable via
// userFromContext) so downstream handlers can attribute the request to a user
// (e.g. job submission labels). Requests that fail to authenticate get a 403
// response matching openapi.yaml's Error schema.
func apiKeyAuthMiddleware(fs *firestore.Client, prefix string, next http.Handler) http.Handler {
	cache := newAPIKeyCache()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, prefix+"/api/") {
			next.ServeHTTP(w, r)
			return
		}

		key, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || key == "" {
			writeError(w, http.StatusForbidden, "FORBIDDEN", "missing or invalid API key")
			return
		}

		user, err := cache.lookup(r.Context(), fs, key)
		if err != nil {
			writeError(w, http.StatusForbidden, "FORBIDDEN", "missing or invalid API key")
			return
		}

		ctx := context.WithValue(r.Context(), userContextKey, user)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
