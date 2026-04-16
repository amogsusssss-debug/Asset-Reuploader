package config

import (
	"bufio"
	"sort"
	"strings"

	"github.com/kartFr/Asset-Reuploader/internal/files"
)

var (
	config        = make(map[string]string, 0)
	defaultConfig = map[string]string{
		"port":        "38073",
		"cookie_file": "cookie.txt",
		"api_key":     "",
	}
)

func init() {
	contents, err := files.Read("config.ini")
	if err == nil {
		scanner := bufio.NewScanner(strings.NewReader(contents))
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			split := strings.SplitN(line, "=", 2)
			if len(split) != 2 {
				continue
			}

			config[split[0]] = split[1]
		}
	}

	for i, v := range defaultConfig {
		if _, exists := config[i]; exists {
			continue
		}
		config[i] = v
	}
}

func Get(key string) string {
	return config[key]
}

func Set(key string, value string) {
	config[key] = value
}

func Save() error {
	var out strings.Builder
	keys := make([]string, 0, len(config))
	for key := range config {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		out.WriteString(key)
		out.WriteByte('=')
		out.WriteString(config[key])
		out.WriteByte('\n')
	}
	return files.Write("config.ini", out.String())
}
