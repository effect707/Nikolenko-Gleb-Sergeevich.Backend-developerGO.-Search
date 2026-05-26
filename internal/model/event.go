package model

import "time"

type SearchEvent struct {
	Query     string    `json:"query"`
	UserID    string    `json:"user_id"`
	Timestamp time.Time `json:"ts"`
	SessionID string    `json:"session_id,omitempty"`
	RequestID string    `json:"request_id,omitempty"`
}

type TopEntry struct {
	Query string `json:"query"`
	Count int64  `json:"count"`
}
