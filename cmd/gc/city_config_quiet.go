package main

import (
	"io"
	"log"
	"sync"

	"github.com/gastownhall/gascity/internal/config"
)

var quietCityConfigLogMu sync.Mutex

// loadCityConfigQuiet performs a full config load while suppressing migration
// warnings emitted through the process logger. Use it for secondary config
// reads, such as scanning registered cities, where warnings would be unrelated
// to the command the user ran.
func loadCityConfigQuiet(cityPath string) (*config.City, error) {
	quietCityConfigLogMu.Lock()
	defer quietCityConfigLogMu.Unlock()

	oldWriter := log.Writer()
	oldFlags := log.Flags()
	oldPrefix := log.Prefix()
	log.SetOutput(io.Discard)
	defer func() {
		log.SetOutput(oldWriter)
		log.SetFlags(oldFlags)
		log.SetPrefix(oldPrefix)
	}()

	return loadCityConfig(cityPath)
}
