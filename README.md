# go-exec

Enterprise cron-style job scheduler written in Go — zero dependencies, single binary.

## Features

- Config format identical to `/etc/crontab`
- Auto PATH resolution across `/usr/local/sbin`, `/usr/sbin`, `/sbin`, etc.
- Structured JSON logging with log rotation (no external dependencies)
- Per-job execution timeout
- Concurrency limiter (max 32 parallel jobs)
- Hot-reload config via `SIGHUP` — no restart needed
- User/group credential switching per job
- Graceful shutdown on `SIGTERM` / `SIGINT`

## Install

```bash
# Download for your platform
curl -fsSL https://github.com/risqinf/go-exec/releases/latest/download/go-exec-linux-amd64 \
  -o /usr/local/bin/go-exec
chmod +x /usr/local/bin/go-exec
```

## Config — `/etc/go-exec`

```
# minute  hour  day  month  weekday  user  command
* * * * * root xp
0 0,1,3,5,6,9,11,12,13,15,17,18,21,23 * * * root backup
```

## Systemd

```bash
make install
systemctl status go-exec
```

## Hot-reload

```bash
# Edit config then:
systemctl reload go-exec
```

## Logs

```bash
# Structured JSON log
tail -f /var/log/go-exec.log | jq

# Or via journald
journalctl -u go-exec -f
```

## Build from source

```bash
git clone https://github.com/risqinf/go-exec
cd go-exec
make build
```

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-config` | `/etc/go-exec` | Config file path |
| `-log` | `/var/log/go-exec.log` | Log file path |
| `-level` | `info` | Log level: `debug\|info\|warn\|error` |
| `-format` | `json` | Log format: `json\|text` |
| `-timeout` | `5m` | Per-job execution timeout |
| `-version` | | Print version and exit |

## License

MIT © risqinf
