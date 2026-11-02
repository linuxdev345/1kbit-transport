package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

type config struct {
	Role              string
	LocalInputAddr    string
	LocalOutputAddr   string
	NetworkListenAddr string
	PeerAddr          string
	RateLimitEnabled  bool
	RateBitsPerSecond int
	MaxDatagram       int
	QueueCapacity     int
}

func loadConfig(path string) (config, error) {
	var cfg config

	file, err := os.Open(path)
	if err != nil {
		return config{}, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "[") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return config{}, fmt.Errorf("%s:%d: expected key = value", path, lineNumber)
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(strings.SplitN(value, "#", 2)[0])
		if strings.HasPrefix(value, "\"") || strings.HasPrefix(value, "'") {
			unquoted, err := parseString(value)
			if err != nil {
				return config{}, fmt.Errorf("%s:%d: %w", path, lineNumber, err)
			}
			value = unquoted
		}
		switch key {
		case "role":
			cfg.Role = strings.ToLower(value)
		case "local_input_addr":
			cfg.LocalInputAddr = value
		case "local_output_addr":
			cfg.LocalOutputAddr = value
		case "network_listen_addr":
			cfg.NetworkListenAddr = value
		case "peer_addr":
			cfg.PeerAddr = value
		case "rate_limit_enabled":
			cfg.RateLimitEnabled, err = strconv.ParseBool(value)
		case "rate_bits_per_second":
			cfg.RateBitsPerSecond, err = strconv.Atoi(value)
		case "max_datagram":
			cfg.MaxDatagram, err = strconv.Atoi(value)
		case "queue_capacity":
			cfg.QueueCapacity, err = strconv.Atoi(value)
		default:
			return config{}, fmt.Errorf("%s:%d: unknown setting %q", path, lineNumber, key)
		}
		if err != nil {
			return config{}, fmt.Errorf("%s:%d: invalid value for %s: %w", path, lineNumber, key, err)
		}
	}
	if err := scanner.Err(); err != nil {
		return config{}, err
	}
	if err := validateConfig(cfg); err != nil {
		return config{}, fmt.Errorf("%s: %w", path, err)
	}
	return cfg, nil
}

func parseString(value string) (string, error) {
	if len(value) < 2 || value[len(value)-1] != value[0] || (value[0] != '\'' && value[0] != '"') {
		return "", fmt.Errorf("invalid string value %q", value)
	}
	if value[0] == '\'' {
		return value[1 : len(value)-1], nil
	}
	parsed, err := strconv.Unquote(value)
	if err != nil {
		return "", fmt.Errorf("invalid string value %q: %w", value, err)
	}
	return parsed, nil
}

func validateConfig(cfg config) error {
	if cfg.Role != "client" && cfg.Role != "server" {
		return fmt.Errorf("role must be client or server")
	}
	if cfg.LocalInputAddr == "" || cfg.LocalOutputAddr == "" || cfg.NetworkListenAddr == "" {
		return fmt.Errorf("local and network addresses are required")
	}
	if _, err := net.ResolveUDPAddr("udp", cfg.LocalInputAddr); err != nil {
		return fmt.Errorf("invalid local_input_addr: %w", err)
	}
	if _, err := net.ResolveUDPAddr("udp", cfg.LocalOutputAddr); err != nil {
		return fmt.Errorf("invalid local_output_addr: %w", err)
	}
	if _, err := net.ResolveUDPAddr("udp", cfg.NetworkListenAddr); err != nil {
		return fmt.Errorf("invalid network_listen_addr: %w", err)
	}
	if cfg.Role == "client" {
		if cfg.PeerAddr == "" {
			return fmt.Errorf("peer_addr is required for client")
		}
		if _, err := net.ResolveUDPAddr("udp", cfg.PeerAddr); err != nil {
			return fmt.Errorf("invalid peer_addr: %w", err)
		}
	}
	if cfg.RateLimitEnabled && cfg.RateBitsPerSecond < 1 {
		return fmt.Errorf("rate_bits_per_second must be positive")
	}
	if cfg.MaxDatagram < 1 || cfg.MaxDatagram > 65507 {
		return fmt.Errorf("max_datagram must be between 1 and 65507")
	}
	if cfg.QueueCapacity < 1 || cfg.QueueCapacity > 4096 {
		return fmt.Errorf("queue_capacity must be between 1 and 4096")
	}
	return nil
}
