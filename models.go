package threadify

import (
	"fmt"
	"strings"
	"time"
)

const (
	defaultConnectTimeout   = 10 * time.Second
	defaultRequestTimeout   = 10 * time.Second
	defaultWaitTimeout      = 5 * time.Second
	defaultMaxInFlight      = 10
	minMaxInFlight          = 1
	maxMaxInFlight          = 100
	defaultProcessedMaxSize = 10_000
	defaultWebSocketURL     = "wss://eng.threadify.dev/threads"
)

type StepStatus string

const (
	StepStatusInProgress StepStatus = "in_progress"
	StepStatusSuccess    StepStatus = "success"
	StepStatusFailed     StepStatus = "failed"
	StepStatusError      StepStatus = "error"
	StepStatusSkipped    StepStatus = "skipped"
)

type ValidationSeverity string

const (
	SeverityInfo     ValidationSeverity = "info"
	SeverityWarning  ValidationSeverity = "warning"
	SeverityCritical ValidationSeverity = "critical"
)

type AccessLevel string

const (
	ForExternal    AccessLevel = "external"
	ForObserver    AccessLevel = "observer"
	ForParticipant AccessLevel = "participant"
)

const (
	ActionConnect           = "connect"
	ActionStartThread       = "startThread"
	ActionJoinThread        = "joinThread"
	ActionRecordThreadEvent = "recordThreadEvent"
	ActionInviteParty       = "inviteParty"
	ActionAddRefs           = "addRefs"
	ActionLinkThread        = "linkThread"
	ActionThreadEnd         = "threadEnd"
	ActionCloseThread       = "closeThread"
	ActionSubscribe         = "subscribe"
	ActionUnsubscribe       = "unsubscribe"
	ActionNotification      = "notification"
	ActionNotificationBatch = "notification_batch"
	ActionAckNotification   = "ack_notification"
	ActionHeartbeat         = "heartbeat"

	// Fields
	FieldAction            = "action"
	FieldStatus            = "status"
	FieldMessage           = "message"
	FieldThreadID          = "threadId"
	FieldStepName          = "stepName"
	FieldSource            = "source"
	FieldUnknown           = "unknown"
	FieldNotificationType  = "notificationType"
	FieldRoleParticipant   = "participant"
	FieldStepID            = "stepId"
	FieldContractName      = "contractName"
	FieldRole              = "role"
	FieldRefs              = "refs"
	FieldEvent             = "event"
	FieldEventTypes        = "eventTypes"
	FieldAckToken          = "ackToken"
	FieldNotificationID    = "notificationId"
	FieldNotificationAckID = "notification_id" // used in ack
	FieldThreadAckID       = "thread_id"       // used in ack
	FieldProcessed         = "processed"
	FieldService           = "serviceName"
	FieldDetails           = "details"
	FieldTimestamp         = "timestamp"
	FieldViolationType     = "violationType"
	FieldOwnerID           = "ownerId"
	FieldAcknowledged      = "acknowledged"
	FieldMaxInFlight       = "maxInFlight"
	FieldAPIKey            = "apiKey"
	FieldSeverity          = "severity"
	FieldThreadToken       = "threadToken"
	FieldStepStatus        = "stepStatus"
	FieldStartedAt         = "startedAt"
	FieldFinishedAt        = "finishedAt"
	FieldContext           = "context"
	FieldSubSteps          = "subSteps"
	FieldIdempotencyKey    = "idempotencyKey"
	FieldIsDuplicate       = "isDuplicate"
	FieldMetadata          = "threadify_metadata"
	FieldReason            = "reason"
	FieldTags              = "tags"
	FieldContractID        = "contractId"
	FieldAccessLevel       = "accessLevel"
	FieldExpiresIn         = "expiresIn"
	FieldExpiresAt         = "expiresAt"
	FieldClosedAt          = "closedAt"
	FieldCompletedAt       = "completedAt"
	FieldCancelledAt       = "cancelledAt"
	FieldThreadStatus      = "threadStatus"
	FieldName              = "name"
	FieldPayload           = "payload"
	FieldRecordedAt        = "recordedAt"

	StatusSuccess    = "success"
	StatusFailed     = "failed"
	StatusError      = "error"
	StatusInProgress = "in_progress"
	StatusViolated   = "violated"
	StatusPassed     = "passed"
	StatusCancelled  = "cancelled"
	StatusCompleted  = "completed"
)

type ConnectOptions struct {
	WSURL          string
	GraphQLURL     string
	Debug          bool
	Logger         Logger
	MaxInFlight    int
	ConnectTimeout time.Duration
	Dialer         Dialer
	ServiceName    string
}

func (o *ConnectOptions) withDefaults() ConnectOptions {
	out := *o

	if out.WSURL == "" {
		out.WSURL = defaultWebSocketURL
	}
	if out.GraphQLURL == "" {
		out.GraphQLURL = deriveGraphQLURL(out.WSURL)
	}
	if out.MaxInFlight == 0 {
		out.MaxInFlight = defaultMaxInFlight
	}
	if out.ConnectTimeout == 0 {
		out.ConnectTimeout = defaultConnectTimeout
	}
	if out.Dialer == nil {
		out.Dialer = &GorillaDialer{}
	}
	return out
}

func (o *ConnectOptions) validate() error {
	if o.WSURL == "" {
		return fmt.Errorf("WSURL is required (use WithWSURL option)")
	}
	if o.MaxInFlight < minMaxInFlight || o.MaxInFlight > maxMaxInFlight {
		return fmt.Errorf("maxInFlight must be between %d and %d", minMaxInFlight, maxMaxInFlight)
	}
	if o.Logger == nil {
		o.Logger = &nopLogger{}
	}
	return nil
}

type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type nopLogger struct{}

func (l *nopLogger) Debug(_ string, _ ...any) {}
func (l *nopLogger) Info(_ string, _ ...any)  {}
func (l *nopLogger) Warn(_ string, _ ...any)  {}
func (l *nopLogger) Error(_ string, _ ...any) {}

type StepResult struct {
	StepName       string `json:"stepName"`
	ThreadID       string `json:"threadId"`
	Status         string `json:"status"`
	IdempotencyKey string `json:"idempotencyKey"`
	Timestamp      string `json:"timestamp"`
	Duplicate      bool   `json:"duplicate,omitempty"`
}

type SubStepData struct {
	Name       string         `json:"name"`
	Status     string         `json:"status"`
	Payload    map[string]any `json:"payload,omitempty"`
	RecordedAt string         `json:"recordedAt"`
}

type InviteOptions struct {
	Role        string
	AccessLevel AccessLevel
	ExpiresIn   string
}

type InviteResponse struct {
	Token       string `json:"token"`
	ThreadID    string `json:"threadId"`
	Role        string `json:"role"`
	AccessLevel string `json:"accessLevel"`
	ExpiresAt   string `json:"expiresAt"`
}

type ThreadEndResponse struct {
	ThreadID string `json:"threadId"`
	Status   string `json:"status"`
	EndedAt  string `json:"endedAt"`
	Message  string `json:"message"`
}

type WaitOptions struct {
	Timeout  time.Duration
	Statuses []string
}

type NotificationData struct {
	NotificationID   string         `json:"notificationId"`
	ThreadID         string         `json:"threadId"`
	StepID           string         `json:"stepId"`
	StepName         string         `json:"stepName"`
	ContractName     string         `json:"contractName"`
	Status           string         `json:"status"`
	StepStatus       string         `json:"stepStatus"`
	Severity         string         `json:"severity"`
	Message          string         `json:"message"`
	Details          map[string]any `json:"details"`
	Timestamp        string         `json:"timestamp"`
	ViolationType    string         `json:"violationType"`
	OwnerID          string         `json:"ownerId"`
	Source           string         `json:"source"`
	NotificationType string         `json:"notificationType"`
}

type ArchivedThreadData struct {
	ID              string `json:"id"`
	ContractID      string `json:"contractId"`
	ContractName    string `json:"contractName"`
	ContractVersion string `json:"contractVersion"`
	OwnerID         string `json:"ownerId"`
	CompanyID       string `json:"companyId"`
	Status          string `json:"status"`
	LastHash        string `json:"lastHash"`
	StartedAt       string `json:"startedAt"`
	CompletedAt     string `json:"completedAt"`
	Error           string `json:"error"`
	Refs            string `json:"refs"`
}

type ArchivedStepData struct {
	ThreadID          string `json:"threadId"`
	StepName          string `json:"stepName"`
	IdempotencyKey    string `json:"idempotencyKey"`
	Status            string `json:"status"`
	RetryCount        int    `json:"retryCount"`
	FirstSeenAt       string `json:"firstSeenAt"`
	LastUpdatedAt     string `json:"lastUpdatedAt"`
	LatestStepID      string `json:"latestStepID"`
	PreviousStep      string `json:"previousStep"`
	Verified          bool   `json:"verified"`
	VerificationError string `json:"verificationError"`
}

type StepHistoryData struct {
	Attempt   int    `json:"attempt"`
	Timestamp string `json:"timestamp"`
	Status    string `json:"status"`
	Context   string `json:"context"`
	Duration  int    `json:"duration"`
	Error     string `json:"error"`
}

type ValidationDetail struct {
	Type     string `json:"type"`
	Severity string `json:"severity"`
	Message  string `json:"message"`
	Field    string `json:"field"`
	Expected string `json:"expected"`
	Actual   string `json:"actual"`
	Rule     string `json:"rule"`
}

type ValidationResult struct {
	ValidationID         string             `json:"validationId"`
	ThreadID             string             `json:"threadId"`
	StepID               string             `json:"stepId"`
	StepName             string             `json:"stepName"`
	IdempotencyKey       string             `json:"idempotencyKey"`
	Timestamp            string             `json:"timestamp"`
	Validations          []ValidationDetail `json:"validations"`
	OverallStatus        string             `json:"overallStatus"`
	HasCriticalViolation bool               `json:"hasCriticalViolation"`
	CriticalCount        int                `json:"criticalCount"`
	WarningCount         int                `json:"warningCount"`
	InfoCount            int                `json:"infoCount"`
}

type HistoryQueryOptions struct {
	Limit        int    `json:"limit,omitempty"`
	Offset       int    `json:"offset,omitempty"`
	StartAt      string `json:"startAt,omitempty"`
	EndAt        string `json:"endAt,omitempty"`
	ActivityType string `json:"activityType,omitempty"`
	Actor        string `json:"actor,omitempty"`
}

type RefQuery struct {
	RefKey        string `json:"refKey"`
	RefValue      string `json:"refValue"`
	Status        string `json:"status,omitempty"`
	StartedAfter  string `json:"startedAfter,omitempty"`
	StartedBefore string `json:"startedBefore,omitempty"`
	Limit         int    `json:"limit,omitempty"`
	Offset        int    `json:"offset,omitempty"`
}

type CompleteDataOptions struct {
	StepHistoryLimit int    `json:"stepHistoryLimit,omitempty"`
	ValidationLimit  int    `json:"validationLimit,omitempty"`
	StepName         string `json:"stepName,omitempty"`
	IdempotencyKey   string `json:"idempotencyKey,omitempty"`
	Status           string `json:"status,omitempty"`
}

func deriveGraphQLURL(wsURL string) string {
	out := strings.Replace(wsURL, "ws://", "http://", 1)
	out = strings.Replace(out, "wss://", "https://", 1)
	return strings.Replace(out, "/threads", "/graphql", 1)
}

func requireNonEmpty(name, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s must be a non-empty string", name)
	}
	return nil
}

func mapStringValues(m map[string]any) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = fmt.Sprintf("%v", v)
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func asString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func asBool(v any) bool {
	if b, ok := v.(bool); ok {
		return b
	}
	return false
}

func asMap(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return nil
}

func asSlice(v any) []any {
	if s, ok := v.([]any); ok {
		return s
	}
	return nil
}

func nowISO() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}
