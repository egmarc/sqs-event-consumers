package main

import (
	"log"
	"os"

	"github.com/egmarc/sqs-event-consumers/internal/worker"
)

func main() {
	if err := worker.Run("db"); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
