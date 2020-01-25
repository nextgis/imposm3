package main

import (
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/nextgis/imposm3"
	"github.com/nextgis/imposm3/cache/query"
	"github.com/nextgis/imposm3/config"
	"github.com/nextgis/imposm3/import_"
	"github.com/nextgis/imposm3/log"
	"github.com/nextgis/imposm3/stats"
	"github.com/nextgis/imposm3/update"
)

func PrintCmds() {
	fmt.Fprintf(os.Stderr, "Usage: %s COMMAND [args]\n\n", os.Args[0])
	fmt.Println("Available commands:")
	fmt.Println("\timport")
	fmt.Println("\tdiff")
	fmt.Println("\trun")
	fmt.Println("\tquery-cache")
	fmt.Println("\tversion")
}

func Main(usage func()) {
	if os.Getenv("GOMAXPROCS") == "" {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}

	if len(os.Args) <= 1 {
		usage()
		os.Exit(1)
	}

	if strings.HasSuffix(os.Args[0], "imposm3") {
		fmt.Println("WARNING: Use imposm binary instead of imposm3!")
	}

	switch os.Args[1] {
	case "import":
		opts := config.ParseImport(os.Args[2:])
		if opts.Base.HTTPProfile != "" {
			stats.StartHTTPPProf(opts.Base.HTTPProfile)
		}
		import_.Import(opts)
	case "diff":
		opts, files := config.ParseDiffImport(os.Args[2:])

		if opts.HTTPProfile != "" {
			stats.StartHTTPPProf(opts.HTTPProfile)
		}
		update.Diff(opts, files)
	case "run":
		opts := config.ParseRunImport(os.Args[2:])

		if opts.HTTPProfile != "" {
			stats.StartHTTPPProf(opts.HTTPProfile)
		}
		update.Run(opts)
	case "query-cache":
		query.Query(os.Args[2:])
	case "version":
		fmt.Println(imposm3.Version)
		os.Exit(0)
	default:
		usage()
		log.Fatalf("invalid command: '%s'", os.Args[1])
	}
	os.Exit(0)

}

func main() {
	Main(PrintCmds)
}
