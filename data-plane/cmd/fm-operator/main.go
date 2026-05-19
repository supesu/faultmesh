package main

import (
	"fmt"

	"github.com/supesu/faultmesh/data-plane/pkg/version"
)

func main() {
	fmt.Println("fm-operator")
	fmt.Printf("Version %s\n", version.Version)
	fmt.Printf("Git commit %s\n", version.GitCommit)
	fmt.Printf("Built %s\n", version.BuildDate)
}
