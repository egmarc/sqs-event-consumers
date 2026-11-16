package main

import "github.com/egmarc/sqs-event-consumers/internal/worker"

func main() { worker.Run("s3") }
