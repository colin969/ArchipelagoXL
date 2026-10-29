package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/securecookie"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/oauth2"
)

type OAuthUser struct {
	ID       string
	Username string
}

// Abstracted so we have room to move away / allow other providers than Discord later on
type OAuthProvider interface {
	Name() string
	Config() *oauth2.Config
	GetUser(ctx context.Context, client *http.Client) (*OAuthUser, error)
	IsAdmin(userID string) bool
}
type OauthHandler struct {
	providers map[string]OAuthProvider
	sessions  *SessionStore
}

func SetupOauth(config *Config, pool *pgxpool.Pool) *OauthHandler {
	providers := map[string]OAuthProvider{}

	if config.OauthDiscordClientId != "" {
		provider, err := newDiscordProvider(config)
		if err != nil {
			log.Printf("failed to setup discord oauth: %v", err)
		} else {
			log.Println("Loaded Discord Oauth")
			providers[provider.Name()] = provider
		}
	}

	if len(providers) == 0 {
		return nil
	}

	current, previous, err := loadOrGenerateSessionKeys(context.Background(), pool)
	if err != nil {
		log.Fatalf("failed to load session keys: %v", err)
	}

	var sessions *SessionStore
	if previous != nil {
		sessions = newSessionStore(
			current.HashKey, current.BlockKey,
			previous.HashKey, previous.BlockKey,
		)
	} else {
		sessions = newSessionStore(current.HashKey, current.BlockKey, nil, nil)
	}

	return &OauthHandler{providers: providers, sessions: sessions}
}

func (o *OauthHandler) RegisterRoutes(r *mux.Router) {
	r.HandleFunc("/auth/{provider}/login", o.handleLogin).Methods(http.MethodGet)
	r.HandleFunc("/auth/{provider}/callback", o.handleCallback).Methods(http.MethodGet)
}

func (o *OauthHandler) handleLogin(w http.ResponseWriter, r *http.Request) {
	p, ok := o.providers[mux.Vars(r)["provider"]]
	if !ok {
		http.Error(w, "unknown provider", http.StatusBadRequest)
		return
	}

	state := randomId(16)
	redirect := r.URL.Query().Get("redirect")
	if !isSafeRedirect(redirect) {
		redirect = "/"
	}

	stateCookie, err := o.sessions.encodeState(map[string]string{
		"state":    state,
		"redirect": redirect,
	})
	if err != nil {
		http.Error(w, "state error", http.StatusInternalServerError)
		return
	}

	http.SetCookie(w, &http.Cookie{
		Name:     "oauth_state",
		Value:    stateCookie,
		MaxAge:   300,
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
	})
	http.Redirect(w, r, p.Config().AuthCodeURL(state), http.StatusTemporaryRedirect)
}

func (o *OauthHandler) handleCallback(w http.ResponseWriter, r *http.Request) {
	p, ok := o.providers[mux.Vars(r)["provider"]]
	if !ok {
		http.Error(w, "unknown provider", http.StatusBadRequest)
		return
	}

	cookie, err := r.Cookie("oauth_state")
	if err != nil {
		http.Error(w, "invalid state", http.StatusBadRequest)
		return
	}

	var stateData map[string]string
	if err := o.sessions.decodeState(cookie.Value, &stateData); err != nil {
		http.Error(w, "invalid state", http.StatusBadRequest)
		return
	}
	if stateData["state"] != r.URL.Query().Get("state") {
		http.Error(w, "invalid state", http.StatusBadRequest)
		return
	}

	redirect := stateData["redirect"]
	if !isSafeRedirect(redirect) {
		redirect = "/"
	}

	token, err := p.Config().Exchange(r.Context(), r.URL.Query().Get("code"))
	if err != nil {
		http.Error(w, "token exchange failed", http.StatusInternalServerError)
		return
	}

	client := p.Config().Client(r.Context(), token)
	user, err := p.GetUser(r.Context(), client)
	if err != nil {
		http.Error(w, "failed to get user", http.StatusInternalServerError)
		return
	}

	session := Session{
		IsLoggedIn: true,
		UserID:     &user.ID,
		IsAdmin:    p.IsAdmin(user.ID),
	}
	if err := o.sessions.Save(w, session); err != nil {
		http.Error(w, "session error", http.StatusInternalServerError)
		return
	}

	// Clear the oauth state cookie
	http.SetCookie(w, &http.Cookie{
		Name:     "oauth_state",
		Value:    "",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
	})

	http.Redirect(w, r, redirect, http.StatusTemporaryRedirect)
}

// Discord provider

type DiscordProvider struct {
	cfg        *oauth2.Config
	adminUsers map[string]struct{}
}

func newDiscordProvider(config *Config) (*DiscordProvider, error) {
	if config.OauthDiscordClientId == "" || config.OauthDiscordClientSecret == "" || config.OauthDiscordRedirectUrl == "" {
		return nil, fmt.Errorf("missing parts of discord oauth config")
	}

	adminUsers := make(map[string]struct{}, len(config.DiscordAdminIds))
	for _, id := range config.DiscordAdminIds {
		adminUsers[id] = struct{}{}
	}

	return &DiscordProvider{
		cfg: &oauth2.Config{
			ClientID:     config.OauthDiscordClientId,
			ClientSecret: config.OauthDiscordClientSecret,
			RedirectURL:  config.OauthDiscordRedirectUrl,
			Scopes:       []string{"identify"},
			Endpoint: oauth2.Endpoint{
				AuthURL:  "https://discord.com/oauth2/authorize",
				TokenURL: "https://discord.com/api/oauth2/token",
			},
		},
		adminUsers: adminUsers,
	}, nil
}

func (d *DiscordProvider) Name() string {
	return "discord"
}

func (d *DiscordProvider) Config() *oauth2.Config {
	return d.cfg
}

func (d *DiscordProvider) IsAdmin(id string) bool {
	_, ok := d.adminUsers[id]
	return ok
}

func (d *DiscordProvider) GetUser(ctx context.Context, client *http.Client) (*OAuthUser, error) {
	resp, err := client.Get("https://discord.com/api/users/@me")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var raw struct {
		ID       string `json:"id"`
		Username string `json:"username"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, err
	}
	return &OAuthUser{ID: raw.ID, Username: raw.Username}, nil
}

// Session store

type Session struct {
	IsAdmin             bool    `json:"is_admin"`
	IsLoggedIn          bool    `json:"is_logged_in"`
	RoomCreationAllowed bool    `json:"room_creation_allowed"`
	UserID              *string `json:"user_id,omitempty"` // Discord ID as string
	RedirectOnLogin     *string `json:"redirect_on_login,omitempty"`
}

type SessionStore struct {
	mu       sync.RWMutex
	current  *securecookie.SecureCookie
	previous *securecookie.SecureCookie
}

func newSessionStore(hashKey, blockKey, prevHashKey, prevBlockKey []byte) *SessionStore {
	s := &SessionStore{
		current: securecookie.New(hashKey, blockKey),
	}
	if prevHashKey != nil && prevBlockKey != nil {
		s.previous = securecookie.New(prevHashKey, prevBlockKey)
	}
	return s
}

func (s *SessionStore) codecs() []securecookie.Codec {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.previous != nil {
		return []securecookie.Codec{s.current, s.previous}
	}
	return []securecookie.Codec{s.current}
}

func (s *SessionStore) Rotate(ctx context.Context, pool *pgxpool.Pool) error {
	k, err := generateAndStoreKey(ctx, pool)
	if err != nil {
		return err
	}

	// Prune unused keys (only keep 2)
	if _, err := pool.Exec(ctx, `
			DELETE FROM session_keys 
			WHERE id NOT IN (
					SELECT id FROM session_keys ORDER BY created_at DESC LIMIT 2
			)
	`); err != nil {
		log.Printf("failed to prune old session keys: %v", err)
	}

	s.mu.Lock()
	s.previous = s.current
	s.current = securecookie.New(k.HashKey, k.BlockKey)
	s.mu.Unlock()

	return nil
}

func (s *SessionStore) Get(r *http.Request) Session {
	cookie, err := r.Cookie("session")
	if err != nil {
		return s.newAnonymous()
	}
	var session Session
	if err := securecookie.DecodeMulti("session", cookie.Value, &session, s.codecs()...); err != nil {
		return s.newAnonymous()
	}
	return session
}

func (s *SessionStore) Save(w http.ResponseWriter, session Session) error {
	s.mu.RLock()
	encoded, err := securecookie.EncodeMulti("session", session, s.current)
	s.mu.RUnlock()
	if err != nil {
		return err
	}
	http.SetCookie(w, &http.Cookie{
		Name:     "session",
		Value:    encoded,
		Expires:  time.Now().Add(31 * 24 * time.Hour),
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
		Path:     "/",
	})
	return nil
}

func (s *SessionStore) encodeState(v any) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return securecookie.EncodeMulti("oauth_state", v, s.current)
}

func (s *SessionStore) decodeState(value string, dst any) error {
	return securecookie.DecodeMulti("oauth_state", value, dst, s.codecs()...)
}

func (s *SessionStore) newAnonymous() Session {
	return Session{}
}

type SessionKeyRecord struct {
	ID       int
	HashKey  []byte
	BlockKey []byte
}

func loadOrGenerateSessionKeys(ctx context.Context, pool *pgxpool.Pool) (current, previous *SessionKeyRecord, err error) {
	// Load up to 2 most recent keys
	rows, err := pool.Query(ctx, `
			SELECT id, hash_key, block_key 
			FROM session_keys 
			ORDER BY created_at DESC 
			LIMIT 2
	`)
	if err != nil {
		return nil, nil, fmt.Errorf("querying session keys: %w", err)
	}
	defer rows.Close()

	var keys []SessionKeyRecord
	for rows.Next() {
		var k SessionKeyRecord
		if err := rows.Scan(&k.ID, &k.HashKey, &k.BlockKey); err != nil {
			return nil, nil, err
		}
		keys = append(keys, k)
	}

	// No keys, generate new one
	if len(keys) == 0 {
		k, err := generateAndStoreKey(ctx, pool)
		if err != nil {
			return nil, nil, err
		}
		return k, nil, nil
	}

	current = &keys[0]
	if len(keys) > 1 {
		previous = &keys[1]
	}
	return current, previous, nil
}

func generateAndStoreKey(ctx context.Context, pool *pgxpool.Pool) (*SessionKeyRecord, error) {
	hashKey := securecookie.GenerateRandomKey(64)
	blockKey := securecookie.GenerateRandomKey(32)
	if hashKey == nil || blockKey == nil {
		return nil, fmt.Errorf("failed to generate random keys")
	}

	var id int
	err := pool.QueryRow(ctx, `
			INSERT INTO session_keys (hash_key, block_key)
			VALUES ($1, $2)
			RETURNING id
	`, hashKey, blockKey).Scan(&id)
	if err != nil {
		return nil, fmt.Errorf("storing session key: %w", err)
	}

	return &SessionKeyRecord{ID: id, HashKey: hashKey, BlockKey: blockKey}, nil
}

// Prevent a malicious login redirect trying to send the user off site afterwards
// To be honest, really should have login called directly or something?
func isSafeRedirect(redirect string) bool {
	if redirect == "" {
		return false
	}
	u, err := url.Parse(redirect)
	if err != nil {
		return false
	}
	return !u.IsAbs() && u.Host == ""
}
