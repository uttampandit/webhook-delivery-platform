package main

import (
	"fmt"
	"os"

	"github.com/uttampandit/webhook-delivery-platform/internal/config"
)

func main() {
	if len(os.Args) > 2 {
		fmt.Fprintln(os.Stderr, "usage: init-env [path]")
		os.Exit(2)
	}
	path := ".env"
	if len(os.Args) == 2 {
		path = os.Args[1]
	}
	if err := config.CreateEnvFile(path); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("Created private environment file: %s\n", path)
}
