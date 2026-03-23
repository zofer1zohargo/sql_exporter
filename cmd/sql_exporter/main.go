package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/burningalchemist/sql_exporter"
	cfg "github.com/burningalchemist/sql_exporter/config"
	_ "github.com/kardianos/minwinsvc"
	"github.com/prometheus/client_golang/prometheus"
	info "github.com/prometheus/client_golang/prometheus/collectors/version"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/version"
	"github.com/prometheus/exporter-toolkit/web"
)

const (
	appName string = "sql_exporter"

	httpReadHeaderTimeout time.Duration = time.Duration(time.Second * 60)
)

var (
	showVersion   = flag.Bool("version", false, "Print version information")
	listenAddress = flag.String("web.listen-address", ":9399", "Address to listen on for web interface and telemetry")
	metricsPath   = flag.String("web.metrics-path", "/metrics", "Path under which to expose metrics")
	enableReload  = flag.Bool("web.enable-reload", false, "Enable reload collector data handler")
	webConfigFile           = flag.String("web.config.file", "", "[EXPERIMENTAL] TLS/BasicAuth configuration file path")
	webHealthListenAddress  = flag.String("web.health.listen-address", "", "When main server uses TLS (web.config.file set), listen here for plain HTTP and proxy to main server /healthz and /ready. K8s probes hit this to avoid TLS handshake errors. Ignored if web.config.file is not set.")
	webHealthTargetURL      = flag.String("web.health.target-url", "", "Main server URL for health proxy (e.g. https://127.0.0.1:9399). Default: https://127.0.0.1:<port> from web.listen-address.")
	webHealthInsecureVerify = flag.Bool("web.health.insecure-skip-verify", true, "Skip TLS verification when health proxy calls the main server. Set false to verify the main server certificate.")
	configFile       = flag.String("config.file", "sql_exporter.yml", "SQL Exporter configuration file path")
	configCheck      = flag.Bool("config.check", false, "Check configuration and exit")
	logFormat     = flag.String("log.format", "logfmt", "Set log output format")
	logLevel      = flag.String("log.level", "info", "Set log level")
	logFile       = flag.String("log.file", "", "Log file to write to, leave empty to write to stderr")
)

func init() {
	prometheus.MustRegister(info.NewCollector("sql_exporter"))
	flag.BoolVar(&cfg.EnablePing, "config.enable-ping", true, "Enable ping for targets")
	flag.BoolVar(&cfg.IgnoreMissingVals, "config.ignore-missing-values",
		false, "[EXPERIMENTAL] Ignore results with missing values for the requested columns")
	flag.StringVar(&cfg.DsnOverride, "config.data-source-name", "",
		"Data source name to override the value in the configuration file with")
	flag.StringVar(&cfg.TargetLabel, "config.target-label", "target", "Target label name")
}

func main() {
	if os.Getenv(cfg.EnvDebug) != "" {
		runtime.SetBlockProfileRate(1)
		runtime.SetMutexProfileFraction(1)
	}

	flag.Parse()

	// Show version and exit.
	if *showVersion {
		fmt.Println(version.Print(appName))
		os.Exit(0)
	}

	// Setup logging.
	logConfig, err := initLogConfig(*logLevel, *logFormat, *logFile)
	if err != nil {
		fmt.Printf("Error initializing exporter: %s\n", err)
		os.Exit(1)
	}

	defer func() {
		if logConfig.logFileHandler != nil {
			err := logConfig.logFileHandler.Close()
			if err != nil {
				fmt.Printf("Error closing log file: %s\n", err)
			}
		}
	}()

	slog.SetDefault(logConfig.logger)

	// Override the config.file default with the SQLEXPORTER_CONFIG environment variable if set.
	if val, ok := os.LookupEnv(cfg.EnvConfigFile); ok {
		*configFile = val
	}

	if *configCheck {
		slog.Info("Checking configuration file", "configFile", *configFile)
		if _, err := cfg.Load(*configFile); err != nil {
			slog.Error("Configuration check failed", "error", err)
			os.Exit(1)
		}
		slog.Info("Configuration check successful")
		os.Exit(0)
	}

	slog.Warn("Starting SQL exporter", "versionInfo", version.Info(), "buildContext",
		version.BuildContext())
	exporter, err := sql_exporter.NewExporter(*configFile, sql_exporter.SvcRegistry)
	if err != nil {
		slog.Error("Error creating exporter", "error", err)
		os.Exit(1)
	}

	// Start the scrape_errors_total metric drop ticker if configured.
	startScrapeErrorsDropTicker(exporter, exporter.Config().Globals.ScrapeErrorDropInterval)

	// Start signal handler to reload collector and target data.
	signalHandler(exporter, *configFile)

	metricsHandler := promhttp.InstrumentMetricHandler(
		prometheus.DefaultRegisterer, ExporterHandlerFor(exporter, sql_exporter.SvcRegistry),
	)

	// Start warmup process if configured, and wrap the metrics handler with the warmup middleware to block /metrics
	// requests until warmup is complete.
	if warmupMiddleware := initWarmup(exporter); warmupMiddleware != nil {
		metricsHandler = warmupMiddleware(metricsHandler)
	}

	// Setup and start webserver.
	// Liveness: no DB connection, no TLS, no log noise. Use for liveness probes.
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	// Readiness: always pings the first target; returns 503 on failure (no logging to avoid log spam).
	http.HandleFunc("/ready", readyHandler(exporter))
	http.HandleFunc("/", HomeHandlerFunc(*metricsPath))
	http.HandleFunc("/config", ConfigHandlerFunc(*metricsPath, exporter))
	http.Handle(*metricsPath, metricsHandler)
	// Expose exporter metrics separately, for debugging purposes.
	http.Handle("/sql_exporter_metrics", promhttp.HandlerFor(prometheus.DefaultGatherer,
		promhttp.HandlerOpts{}))
	// Expose refresh handler to reload collectors and targets
	if *enableReload {
		http.HandleFunc("/reload", reloadHandler(exporter, *configFile))
	}

	// Health proxy server: only when main server uses TLS. Thin plain HTTP server that forwards /healthz and
	// /ready to the main server over HTTPS, so K8s can probe without TLS handshake errors.
	if *webConfigFile != "" && *webHealthListenAddress != "" {
		targetURL := *webHealthTargetURL
		if targetURL == "" {
			_, port, err := net.SplitHostPort(*listenAddress)
			if err != nil {
				slog.Error("Cannot derive health target URL from listen address", "web.listen-address", *listenAddress, "error", err)
				os.Exit(1)
			}
			targetURL = "https://127.0.0.1:" + port
		}
		proxy := newHealthProxy(targetURL, *webHealthInsecureVerify)
		healthMux := http.NewServeMux()
		healthMux.HandleFunc("/healthz", proxy.serve)
		healthMux.HandleFunc("/ready", proxy.serve)
		healthServer := &http.Server{Addr: *webHealthListenAddress, Handler: healthMux, ReadHeaderTimeout: httpReadHeaderTimeout}
		go func() {
			if err := healthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				slog.Error("Health server error", "error", err)
			}
		}()
		slog.Info("Health server listening (plain HTTP, proxies to main)", "address", *webHealthListenAddress, "target", targetURL)
	}

	server := &http.Server{Addr: *listenAddress, ReadHeaderTimeout: httpReadHeaderTimeout}
	if err := web.ListenAndServe(server, &web.FlagConfig{
		WebListenAddresses: &([]string{*listenAddress}),
		WebConfigFile:      webConfigFile, WebSystemdSocket: OfBool(false),
	}, logConfig.logger); err != nil {
		slog.Error("Error starting web server", "error", err)
		os.Exit(1)

	}
}

const (
	readyTimeout     = 2 * time.Second
	healthProxyTimeout = 5 * time.Second
)

// healthProxy forwards /healthz and /ready to the main server over HTTPS.
type healthProxy struct {
	client  *http.Client
	baseURL string
}

func newHealthProxy(targetBaseURL string, insecureSkipVerify bool) *healthProxy {
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: insecureSkipVerify},
	}
	return &healthProxy{
		client:  &http.Client{Timeout: healthProxyTimeout, Transport: transport},
		baseURL: targetBaseURL,
	}
}

func (p *healthProxy) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	targetURL := p.baseURL + r.URL.Path
	req, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL, nil)
	if err != nil {
		slog.Error("Health proxy request build failed", "url", targetURL, "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	resp, err := p.client.Do(req)
	if err != nil {
		slog.Error("Health proxy request failed", "url", targetURL, "error", err)
		w.WriteHeader(http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	if resp.Body != nil && r.Method != http.MethodHead {
		_, _ = io.Copy(w, resp.Body)
	}
}

// readyHandler returns a handler for the /ready readiness probe. It pings all targets; returns 503 and logs which
// targets failed when any are unreachable or misconfigured.
func readyHandler(e sql_exporter.Exporter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), readyTimeout)
		defer cancel()
		if err := e.CheckReady(ctx); err != nil {
			slog.Warn("Readiness check failed: one or more targets unreachable or misconfigured", "error", err)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

// reloadHandler returns a handler that reloads collector and target data.
func reloadHandler(e sql_exporter.Exporter, configFile string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := sql_exporter.Reload(e, &configFile); err != nil {
			slog.Error("Error reloading collector and target data", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

// signalHandler listens for SIGHUP signals and reloads the collector and target data.
func signalHandler(e sql_exporter.Exporter, configFile string) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP)
	go func() {
		for range c {
			if err := sql_exporter.Reload(e, &configFile); err != nil {
				slog.Error("Error reloading collector and target data", "error", err)
			}
		}
	}()
}

// startScrapeErrorsDropTicker starts a ticker that periodically drops scrape error metrics.
func startScrapeErrorsDropTicker(exporter sql_exporter.Exporter, interval model.Duration) {
	if interval <= 0 {
		return
	}

	ticker := time.NewTicker(time.Duration(interval))
	slog.Warn("Started scrape_errors_total metrics drop ticker", "interval", interval)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			exporter.DropErrorMetrics()
		}
	}()
}
