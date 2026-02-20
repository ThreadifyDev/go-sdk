package threadify

import (
	"fmt"
	"time"
)

type Notification struct {
	NotificationID   string
	ThreadID         string
	StepID           string
	StepName         string
	ContractName     string
	Status           string // "passed", "violated", "none"
	StepStatus       string // "success", "failed", "error"
	Severity         string // "info", "warning", "critical"
	Message          string
	Details          map[string]any
	Timestamp        time.Time
	ViolationType    string
	OwnerID          string
	Source           string // "execution", "validation", "thread"
	NotificationType string

	ackToken     string
	conn         *Connection
	acknowledged bool
}

func NewNotification(data map[string]any, conn *Connection, ackToken string) *Notification {
	ts := time.Now().UTC()
	if tsStr := asString(data[FieldTimestamp]); tsStr != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, tsStr); err == nil {
			ts = parsed
		}
	}

	return &Notification{
		NotificationID:   asString(data[FieldNotificationID]),
		ThreadID:         asString(data[FieldThreadID]),
		StepID:           asString(data[FieldStepID]),
		StepName:         asString(data[FieldStepName]),
		ContractName:     asString(data[FieldContractName]),
		Status:           asString(data[FieldStatus]),
		StepStatus:       asString(data[FieldStepStatus]),
		Severity:         asString(data[FieldSeverity]),
		Message:          asString(data[FieldMessage]),
		Details:          asMap(data[FieldDetails]),
		Timestamp:        ts,
		ViolationType:    asString(data[FieldViolationType]),
		OwnerID:          asString(data[FieldOwnerID]),
		Source:           asString(data[FieldSource]),
		NotificationType: asString(data[FieldNotificationType]),
		ackToken:         ackToken,
		conn:             conn,
	}
}

func (n *Notification) Ack() error {
	if n.acknowledged {
		n.conn.logger.Debug("Notification already acknowledged", "notificationID", n.NotificationID)
		return nil
	}

	if n.ackToken == "" {
		return fmt.Errorf("cannot ACK notification %s: ackToken is required", n.NotificationID)
	}

	n.acknowledged = true
	n.conn.sendAck(n.NotificationID, n.ThreadID, n.ackToken)
	return nil
}

func (n *Notification) IsAcknowledged() bool {
	return n.acknowledged
}

func (n *Notification) IsViolated() bool {
	return n.Status == StatusViolated
}

func (n *Notification) IsPassed() bool {
	return n.Status == StatusPassed
}

func (n *Notification) IsCritical() bool {
	return n.Severity == string(SeverityCritical)
}
func (n *Notification) IsWarning() bool {
	return n.Severity == string(SeverityWarning)
}

func (n *Notification) IsInfo() bool {
	return n.Severity == string(SeverityInfo)
}
func (n *Notification) IsSuccess() bool {
	return n.StepStatus == StatusSuccess
}

func (n *Notification) IsFailed() bool {
	return n.StepStatus == StatusFailed
}
func (n *Notification) IsError() bool {
	return n.StepStatus == StatusError
}

func (n *Notification) String() string {
	sev := n.Severity
	if sev == "" {
		sev = FieldUnknown
	}
	return fmt.Sprintf("[%s] %s: %s", sev, n.StepName, n.Message)
}

func (n *Notification) ToMap() map[string]any {
	return map[string]any{
		FieldNotificationID: n.NotificationID,
		FieldThreadID:       n.ThreadID,
		FieldStepID:         n.StepID,
		FieldStepName:       n.StepName,
		FieldContractName:   n.ContractName,
		FieldStatus:         n.Status,
		FieldStepStatus:     n.StepStatus,
		FieldSeverity:       n.Severity,
		FieldMessage:        n.Message,
		FieldDetails:        n.Details,
		FieldTimestamp:      n.Timestamp.Format(time.RFC3339Nano),
		FieldViolationType:  n.ViolationType,
		FieldOwnerID:        n.OwnerID,
		FieldAcknowledged:   n.acknowledged,
	}
}
