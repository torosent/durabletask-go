package helpers

import (
	"fmt"
	"os"

	"github.com/google/uuid"
)

func GetDefaultWorkerName() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	pid := os.Getpid()
	u, err := uuid.NewV7()
	var uuidStr string
	if err == nil {
		uuidStr = u.String()
	} else {
		uuidStr = uuid.NewString()
	}
	return fmt.Sprintf("%v,%d,%v", hostname, pid, uuidStr)
}
