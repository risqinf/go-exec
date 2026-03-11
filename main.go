// go-exec — Enterprise cron-style job scheduler
// Repository : https://github.com/risqinf/go-exec
// License    : MIT
package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// Version
// ─────────────────────────────────────────────────────────────────────────────

var (
	Version   = "dev"
	BuildTime = "unknown"
	GitCommit = "unknown"
)

func versionString() string {
	return fmt.Sprintf("go-exec version=%s build_time=%s git_commit=%s",
		Version, BuildTime, GitCommit)
}

// ─────────────────────────────────────────────────────────────────────────────
// Logger
// ─────────────────────────────────────────────────────────────────────────────

type logLevel int

const (
	levelDebug logLevel = iota
	levelInfo
	levelWarn
	levelError
	levelFatal
)

var levelNames = map[logLevel]string{
	levelDebug: "DEBUG",
	levelInfo:  "INFO",
	levelWarn:  "WARN",
	levelError: "ERROR",
	levelFatal: "FATAL",
}

type Logger struct {
	mu       sync.Mutex
	level    logLevel
	format   string // "json" | "text"
	writers  []io.Writer
	fields   map[string]string // static fields appended to every entry
}

type loggerConfig struct {
	Level      string
	Format     string
	FilePath   string
	MaxSizeMB  int64
	MaxBackups int
	Service    string
	Version    string
}

func newLogger(cfg loggerConfig) (*Logger, error) {
	// ── parse level ───────────────────────────────────────────────────────────
	lvlMap := map[string]logLevel{
		"debug": levelDebug,
		"info":  levelInfo,
		"warn":  levelWarn,
		"error": levelError,
	}
	lv, ok := lvlMap[strings.ToLower(cfg.Level)]
	if !ok {
		return nil, fmt.Errorf("invalid log level %q", cfg.Level)
	}

	// ── rotating file writer ──────────────────────────────────────────────────
	rw, err := newRotatingWriter(cfg.FilePath, cfg.MaxSizeMB, cfg.MaxBackups)
	if err != nil {
		return nil, fmt.Errorf("open log file %q: %w", cfg.FilePath, err)
	}

	// ── static fields ─────────────────────────────────────────────────────────
	hostname, _ := os.Hostname()
	fields := map[string]string{
		"service":  cfg.Service,
		"version":  cfg.Version,
		"hostname": hostname,
		"pid":      strconv.Itoa(os.Getpid()),
	}

	return &Logger{
		level:   lv,
		format:  cfg.Format,
		writers: []io.Writer{rw, os.Stdout},
		fields:  fields,
	}, nil
}

func (l *Logger) log(lv logLevel, msg string, kv ...any) {
	if lv < l.level {
		return
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	extra := kvToMap(kv)

	var line string
	if l.format == "json" {
		line = l.formatJSON(now, lv, msg, extra)
	} else {
		line = l.formatText(now, lv, msg, extra)
	}

	l.mu.Lock()
	for _, w := range l.writers {
		_, _ = fmt.Fprintln(w, line)
	}
	l.mu.Unlock()

	if lv == levelFatal {
		os.Exit(1)
	}
}

func (l *Logger) formatJSON(ts string, lv logLevel, msg string, extra map[string]any) string {
	var b strings.Builder
	b.WriteString(`{"timestamp":"`)
	b.WriteString(ts)
	b.WriteString(`","level":"`)
	b.WriteString(strings.ToLower(levelNames[lv]))
	b.WriteString(`","message":`)
	b.WriteString(jsonQuote(msg))

	// static fields
	for k, v := range l.fields {
		b.WriteString(`,"`)
		b.WriteString(k)
		b.WriteString(`":`)
		b.WriteString(jsonQuote(v))
	}
	// dynamic fields
	for k, v := range extra {
		b.WriteString(`,"`)
		b.WriteString(k)
		b.WriteString(`":`)
		b.WriteString(jsonQuote(fmt.Sprintf("%v", v)))
	}
	b.WriteString("}")
	return b.String()
}

func (l *Logger) formatText(ts string, lv logLevel, msg string, extra map[string]any) string {
	var b strings.Builder
	b.WriteString(ts)
	b.WriteString(" [")
	b.WriteString(levelNames[lv])
	b.WriteString("] ")
	b.WriteString(msg)
	for k, v := range extra {
		b.WriteString(" ")
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(fmt.Sprintf("%v", v))
	}
	return b.String()
}

func (l *Logger) Debug(msg string, kv ...any) { l.log(levelDebug, msg, kv...) }
func (l *Logger) Info(msg string, kv ...any)  { l.log(levelInfo, msg, kv...) }
func (l *Logger) Warn(msg string, kv ...any)  { l.log(levelWarn, msg, kv...) }
func (l *Logger) Error(msg string, kv ...any) { l.log(levelError, msg, kv...) }
func (l *Logger) Fatal(msg string, kv ...any) { l.log(levelFatal, msg, kv...) }

func (l *Logger) With(kv ...any) *Logger {
	child := &Logger{
		level:   l.level,
		format:  l.format,
		writers: l.writers,
		fields:  make(map[string]string, len(l.fields)),
	}
	for k, v := range l.fields {
		child.fields[k] = v
	}
	extra := kvToMap(kv)
	for k, v := range extra {
		child.fields[k] = fmt.Sprintf("%v", v)
	}
	return child
}

// ── Rotating file writer ──────────────────────────────────────────────────────

type rotatingWriter struct {
	mu         sync.Mutex
	path       string
	maxBytes   int64
	maxBackups int
	file       *os.File
	size       int64
}

func newRotatingWriter(path string, maxMB int64, maxBackups int) (*rotatingWriter, error) {
	if maxMB <= 0 {
		maxMB = 100
	}
	if maxBackups <= 0 {
		maxBackups = 7
	}
	rw := &rotatingWriter{
		path:       path,
		maxBytes:   maxMB * 1024 * 1024,
		maxBackups: maxBackups,
	}
	if err := rw.openOrCreate(); err != nil {
		return nil, err
	}
	return rw, nil
}

func (rw *rotatingWriter) openOrCreate() error {
	_ = os.MkdirAll(filepath.Dir(rw.path), 0755)
	f, err := os.OpenFile(rw.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		return err
	}
	rw.file = f
	rw.size = info.Size()
	return nil
}

func (rw *rotatingWriter) Write(p []byte) (int, error) {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	if rw.size+int64(len(p)) > rw.maxBytes {
		_ = rw.rotate()
	}

	n, err := rw.file.Write(p)
	rw.size += int64(n)
	return n, err
}

func (rw *rotatingWriter) rotate() error {
	_ = rw.file.Close()

	// Shift backups: .7 deleted, .6→.7, …, .1→.2
	for i := rw.maxBackups - 1; i >= 1; i-- {
		old := fmt.Sprintf("%s.%d", rw.path, i)
		newer := fmt.Sprintf("%s.%d", rw.path, i+1)
		_ = os.Rename(old, newer)
	}
	_ = os.Rename(rw.path, rw.path+".1")

	return rw.openOrCreate()
}

// ─────────────────────────────────────────────────────────────────────────────
// Config
// ─────────────────────────────────────────────────────────────────────────────

var defaultSearchPaths = []string{
	"/usr/local/sbin",
	"/usr/local/bin",
	"/usr/sbin",
	"/usr/bin",
	"/sbin",
	"/bin",
}

// Job is a single scheduled task.
type Job struct {
	Raw     string
	Minute  string
	Hour    string
	Day     string
	Month   string
	Weekday string
	User    string
	Command string
	Args    []string
}

func (j Job) String() string {
	return fmt.Sprintf("schedule=%s_%s_%s_%s_%s user=%s cmd=%s args=%v",
		j.Minute, j.Hour, j.Day, j.Month, j.Weekday, j.User, j.Command, j.Args)
}

// Config holds the full parsed configuration.
type Config struct {
	Jobs []Job
}

func loadConfig(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open config %q: %w", path, err)
	}
	defer f.Close()

	cfg := &Config{}
	lineNum := 0
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		job, err := parseLine(line)
		if err != nil {
			return nil, fmt.Errorf("config %q line %d: %w", path, lineNum, err)
		}
		cfg.Jobs = append(cfg.Jobs, job)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}
	return cfg, nil
}

func parseLine(line string) (Job, error) {
	fields := strings.Fields(line)
	if len(fields) < 7 {
		return Job{}, fmt.Errorf(
			"need ≥7 fields (min hour day month weekday user command), got %d in %q",
			len(fields), line,
		)
	}
	job := Job{
		Raw:     line,
		Minute:  fields[0],
		Hour:    fields[1],
		Day:     fields[2],
		Month:   fields[3],
		Weekday: fields[4],
		User:    fields[5],
		Command: fields[6],
	}
	if len(fields) > 7 {
		job.Args = fields[7:]
	}
	for name, val := range map[string]string{
		"minute": job.Minute, "hour": job.Hour, "day": job.Day,
		"month": job.Month, "weekday": job.Weekday,
		"user": job.User, "command": job.Command,
	} {
		if val == "" {
			return Job{}, fmt.Errorf("field %q is empty", name)
		}
	}
	return job, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Executor
// ─────────────────────────────────────────────────────────────────────────────

const defaultJobTimeout = 5 * time.Minute

// Result holds the outcome of one job run.
type Result struct {
	Job        Job
	StartedAt  time.Time
	FinishedAt time.Time
	Duration   time.Duration
	ExitCode   int
	Stdout     string
	Stderr     string
	Error      error
}

func (r Result) Success() bool { return r.Error == nil && r.ExitCode == 0 }

type Executor struct {
	searchPaths []string
	timeout     time.Duration
}

func newExecutor(paths []string, timeout time.Duration) *Executor {
	if len(paths) == 0 {
		paths = defaultSearchPaths
	}
	if timeout <= 0 {
		timeout = defaultJobTimeout
	}
	return &Executor{searchPaths: paths, timeout: timeout}
}

func (e *Executor) Run(parentCtx context.Context, job Job) Result {
	res := Result{Job: job, StartedAt: time.Now()}

	cmdPath, err := e.resolvePath(job.Command)
	if err != nil {
		res.Error = fmt.Errorf("resolve %q: %w", job.Command, err)
		res.FinishedAt = time.Now()
		res.Duration = res.FinishedAt.Sub(res.StartedAt)
		return res
	}

	ctx, cancel := context.WithTimeout(parentCtx, e.timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, cmdPath, job.Args...)
	cmd.Env = e.buildEnv()

	if err := e.setCredentials(cmd, job.User); err != nil {
		res.Error = fmt.Errorf("credentials user=%q: %w", job.User, err)
		res.FinishedAt = time.Now()
		res.Duration = res.FinishedAt.Sub(res.StartedAt)
		return res
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	runErr := cmd.Run()
	res.FinishedAt = time.Now()
	res.Duration   = res.FinishedAt.Sub(res.StartedAt)
	res.Stdout     = strings.TrimSpace(stdout.String())
	res.Stderr     = strings.TrimSpace(stderr.String())

	if runErr != nil {
		if exitErr, ok := runErr.(*exec.ExitError); ok {
			res.ExitCode = exitErr.ExitCode()
		} else {
			res.ExitCode = -1
		}
		if ctx.Err() == context.DeadlineExceeded {
			res.Error = fmt.Errorf("timeout after %s", e.timeout)
		} else {
			res.Error = runErr
		}
	}
	return res
}

func (e *Executor) resolvePath(cmd string) (string, error) {
	if filepath.IsAbs(cmd) {
		if _, err := os.Stat(cmd); err != nil {
			return "", fmt.Errorf("not found: %s", cmd)
		}
		return cmd, nil
	}
	for _, dir := range e.searchPaths {
		full := filepath.Join(dir, cmd)
		if info, err := os.Stat(full); err == nil && !info.IsDir() {
			return full, nil
		}
	}
	if p, err := exec.LookPath(cmd); err == nil {
		return p, nil
	}
	return "", fmt.Errorf("command %q not found in %s",
		cmd, strings.Join(e.searchPaths, ":"))
}

func (e *Executor) setCredentials(cmd *exec.Cmd, username string) error {
	if username == "" || username == "root" {
		return nil
	}
	u, err := user.Lookup(username)
	if err != nil {
		return fmt.Errorf("lookup %q: %w", username, err)
	}
	uid, _ := strconv.Atoi(u.Uid)
	gid, _ := strconv.Atoi(u.Gid)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Credential: &syscall.Credential{
			Uid: uint32(uid),
			Gid: uint32(gid),
		},
	}
	return nil
}

func (e *Executor) buildEnv() []string {
	path := strings.Join(e.searchPaths, ":") + ":" + os.Getenv("PATH")
	env := []string{
		"PATH=" + path,
		"HOME=/root",
		"SHELL=/bin/bash",
		"LANG=en_US.UTF-8",
	}
	for _, k := range []string{"TZ", "TERM"} {
		if v := os.Getenv(k); v != "" {
			env = append(env, k+"="+v)
		}
	}
	return env
}

// ─────────────────────────────────────────────────────────────────────────────
// Scheduler
// ─────────────────────────────────────────────────────────────────────────────

const maxConcurrentJobs = 32

type Scheduler struct {
	log    *Logger
	exec   *Executor
	mu     sync.RWMutex
	cfg    *Config
	sem    chan struct{}
	stopCh chan struct{}
	wg     sync.WaitGroup
}

func newScheduler(log *Logger, exec *Executor) *Scheduler {
	return &Scheduler{
		log:    log,
		exec:   exec,
		sem:    make(chan struct{}, maxConcurrentJobs),
		stopCh: make(chan struct{}),
	}
}

func (s *Scheduler) Start(ctx context.Context, cfg *Config) {
	s.mu.Lock()
	s.cfg = cfg
	s.mu.Unlock()

	s.wg.Add(1)
	go s.loop(ctx)

	s.log.Info("scheduler started",
		"max_concurrent_jobs", maxConcurrentJobs,
		"jobs", len(cfg.Jobs),
	)
}

func (s *Scheduler) Reload(cfg *Config) {
	s.mu.Lock()
	s.cfg = cfg
	s.mu.Unlock()
}

func (s *Scheduler) Stop() {
	close(s.stopCh)
	s.wg.Wait()
}

func (s *Scheduler) loop(ctx context.Context) {
	defer s.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	lastMinute := -1

	for {
		select {
		case <-s.stopCh:
			return
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if now.Minute() == lastMinute {
				continue
			}
			lastMinute = now.Minute()
			s.dispatch(ctx, now)
		}
	}
}

func (s *Scheduler) dispatch(ctx context.Context, now time.Time) {
	s.mu.RLock()
	jobs := s.cfg.Jobs
	s.mu.RUnlock()

	fired := 0
	for _, job := range jobs {
		if !cronMatches(job, now) {
			continue
		}
		fired++
		j := job

		select {
		case s.sem <- struct{}{}:
		default:
			s.log.Warn("semaphore full — job skipped",
				"job", j.String(),
				"max_concurrent", maxConcurrentJobs,
			)
			continue
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer func() { <-s.sem }()
			s.runJob(ctx, j)
		}()
	}

	if fired > 0 {
		s.log.Debug("dispatch cycle",
			"time", now.Format(time.RFC3339),
			"fired", fired,
			"total_jobs", len(jobs),
		)
	}
}

func (s *Scheduler) runJob(ctx context.Context, job Job) {
	jlog := s.log.With(
		"job_cmd", job.Command,
		"job_user", job.User,
		"job_schedule", fmt.Sprintf("%s %s %s %s %s",
			job.Minute, job.Hour, job.Day, job.Month, job.Weekday),
	)
	jlog.Info("job started")

	res := s.exec.Run(ctx, job)

	fields := []any{
		"duration_ms", res.Duration.Milliseconds(),
		"exit_code", res.ExitCode,
	}
	if res.Stdout != "" {
		fields = append(fields, "stdout", res.Stdout)
	}
	if res.Stderr != "" {
		fields = append(fields, "stderr", res.Stderr)
	}
	if res.Success() {
		jlog.Info("job completed", fields...)
	} else {
		fields = append(fields, "error", res.Error)
		jlog.Error("job failed", fields...)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Cron expression matching
// ─────────────────────────────────────────────────────────────────────────────

func cronMatches(job Job, t time.Time) bool {
	return matchField(job.Minute, t.Minute()) &&
		matchField(job.Hour, t.Hour()) &&
		matchField(job.Day, t.Day()) &&
		matchField(job.Month, int(t.Month())) &&
		matchField(job.Weekday, int(t.Weekday()))
}

func matchField(field string, value int) bool {
	if field == "*" {
		return true
	}
	for _, part := range strings.Split(field, ",") {
		if matchPart(strings.TrimSpace(part), value) {
			return true
		}
	}
	return false
}

func matchPart(part string, value int) bool {
	// step: */5 or 1-30/5
	if idx := strings.Index(part, "/"); idx != -1 {
		step, err := strconv.Atoi(part[idx+1:])
		if err != nil || step <= 0 {
			return false
		}
		base := part[:idx]
		if base == "*" || base == "0" {
			return value%step == 0
		}
		start, end, ok := parseRange(base)
		return ok && value >= start && value <= end && (value-start)%step == 0
	}
	// range: 1-5
	if start, end, ok := parseRange(part); ok {
		return value >= start && value <= end
	}
	// exact
	n, err := strconv.Atoi(part)
	return err == nil && n == value
}

func parseRange(s string) (start, end int, ok bool) {
	p := strings.SplitN(s, "-", 2)
	if len(p) != 2 {
		return 0, 0, false
	}
	a, err1 := strconv.Atoi(p[0])
	b, err2 := strconv.Atoi(p[1])
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return a, b, true
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func kvToMap(kv []any) map[string]any {
	m := make(map[string]any, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		key, ok := kv[i].(string)
		if !ok {
			key = fmt.Sprintf("%v", kv[i])
		}
		m[key] = kv[i+1]
	}
	return m
}

func jsonQuote(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	s = strings.ReplaceAll(s, "\r", `\r`)
	s = strings.ReplaceAll(s, "\t", `\t`)
	return `"` + s + `"`
}

// ─────────────────────────────────────────────────────────────────────────────
// main
// ─────────────────────────────────────────────────────────────────────────────

func main() {
	configPath := flag.String("config", "/etc/go-exec", "Path to config file")
	logPath    := flag.String("log", "/var/log/go-exec.log", "Path to log file")
	logLevel   := flag.String("level", "info", "Log level: debug|info|warn|error")
	logFormat  := flag.String("format", "json", "Log format: json|text")
	jobTimeout := flag.Duration("timeout", defaultJobTimeout, "Per-job execution timeout")
	showVer    := flag.Bool("version", false, "Print version and exit")
	flag.Parse()

	if *showVer {
		fmt.Println(versionString())
		os.Exit(0)
	}

	// ── Logger ─────────────────────────────────────────────────────────────
	hostname, _ := os.Hostname()
	_ = hostname

	log, err := newLogger(loggerConfig{
		Level:      *logLevel,
		Format:     *logFormat,
		FilePath:   *logPath,
		MaxSizeMB:  100,
		MaxBackups: 7,
		Service:    "go-exec",
		Version:    Version,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL init logger: %v\n", err)
		os.Exit(1)
	}

	log.Info("service starting",
		"version", Version,
		"build_time", BuildTime,
		"git_commit", GitCommit,
		"config", *configPath,
		"log_file", *logPath,
		"log_level", *logLevel,
		"log_format", *logFormat,
		"job_timeout", jobTimeout.String(),
	)

	// ── Config ─────────────────────────────────────────────────────────────
	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatal("failed to load config", "path", *configPath, "error", err)
	}
	log.Info("config loaded", "path", *configPath, "jobs", len(cfg.Jobs))
	for i, j := range cfg.Jobs {
		log.Debug("registered job", "index", i, "job", j.String())
	}

	// ── Scheduler ──────────────────────────────────────────────────────────
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	exec := newExecutor(defaultSearchPaths, *jobTimeout)
	sched := newScheduler(log, exec)
	sched.Start(ctx, cfg)

	// ── Signal handling ────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	for {
		sig := <-quit
		log.Info("signal received", "signal", sig.String())

		switch sig {
		case syscall.SIGHUP:
			newCfg, err := loadConfig(*configPath)
			if err != nil {
				log.Error("config reload failed", "path", *configPath, "error", err)
				continue
			}
			sched.Reload(newCfg)
			log.Info("config reloaded", "path", *configPath, "jobs", len(newCfg.Jobs))

		case syscall.SIGINT, syscall.SIGTERM:
			log.Info("initiating graceful shutdown")
			cancel()
			sched.Stop()
			log.Info("service stopped gracefully")
			return
		}
	}
}
