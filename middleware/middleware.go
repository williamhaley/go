package server

import (
	"net/http"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// NewAuthMiddleware returns an http.Handler to authenticate all server requests
func NewAuthMiddleware(accessToken string) func(http.Handler) http.Handler {
	hashedAccessToken := getHashedAccessToken(accessToken)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			accessToken := r.Header.Get("Access-Token")
			if accessToken == "" {
				accessToken = r.URL.Query().Get("Access-Token")
			}

			if err := bcrypt.CompareHashAndPassword(hashedAccessToken, []byte(accessToken)); err != nil && err == bcrypt.ErrMismatchedHashAndPassword {
				w.WriteHeader(http.StatusForbidden)
				w.Write([]byte("no"))

				return
			} else if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("no"))

				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

func getHashedAccessToken(accessToken string) []byte {
	cost := 1
	sufficientStrengthFound := false
	var hashedAccessToken []byte

	for !sufficientStrengthFound {
		start := time.Now()
		var err error
		hashedAccessToken, err = bcrypt.GenerateFromPassword([]byte(accessToken), cost)
		if err != nil {
			panic(err)
		}

		if time.Since(start) > time.Millisecond*50 {
			sufficientStrengthFound = true
		} else {
			cost++
		}
	}

	return hashedAccessToken
}
