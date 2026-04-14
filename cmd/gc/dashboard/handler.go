package dashboard

import (
	"embed"
	"html"
	"io/fs"
	"log"
	"net/http"
	"strings"
)

//go:embed static
var staticFiles embed.FS

// NewDashboardMux creates an HTTP handler that serves the static dashboard.
// All API operations go from the browser directly to the supervisor via WebSocket.
func NewDashboardMux(apiURL, initialCityScope string) (http.Handler, error) {

	staticFS, err := fs.Sub(staticFiles, "static")
	if err != nil {
		return nil, err
	}
	staticHandler := http.FileServer(http.FS(staticFS))

	mux := http.NewServeMux()
	mux.Handle("/static/", http.StripPrefix("/static/", staticHandler))
	// Serve index.html for all non-static paths.
	dashAPIURL := apiURL // capture for closure
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		indexData, err := fs.ReadFile(staticFS, "index.html")
		if err != nil {
			http.Error(w, "dashboard not found", http.StatusInternalServerError)
			return
		}
		// Inject API URL and selected city into the static HTML.
		htmlStr := string(indexData)
		city := r.URL.Query().Get("city")
		if city == "" {
			city = initialCityScope
		}
		inject := ""
		if dashAPIURL != "" {
			inject += `<meta name="api-url" content="` + html.EscapeString(dashAPIURL) + `">`
		}
		if city != "" {
			inject += `<meta name="selected-city" content="` + html.EscapeString(city) + `">`
		}
		if inject != "" {
			htmlStr = strings.Replace(htmlStr, "</head>", inject+"\n</head>", 1)
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if _, err := w.Write([]byte(htmlStr)); err != nil {
			log.Printf("dashboard: response write failed: %v", err)
		}
	})

	return mux, nil
}
