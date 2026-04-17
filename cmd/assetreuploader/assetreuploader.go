package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/kartFr/Asset-Reuploader/internal/app/config"
	"github.com/kartFr/Asset-Reuploader/internal/color"
	"github.com/kartFr/Asset-Reuploader/internal/console"
	"github.com/kartFr/Asset-Reuploader/internal/files"
	"github.com/kartFr/Asset-Reuploader/internal/roblox"
)

var (
	cookieFile = config.Get("cookie_file")
	port       = config.Get("port")
)

func main() {
	console.ClearScreen()

	fmt.Println("Authenticating cookie...")

	cookie, readErr := files.Read(cookieFile)
	
	// Parse cookie file to extract Roblox cookie and API key
	roblosCookie, apiKey := parseCookieFile(cookie)
	roblosCookie = strings.TrimSpace(roblosCookie)

	c, clientErr := roblox.NewClient(roblosCookie)
	console.ClearScreen()

	if readErr != nil || clientErr != nil {
		if readErr != nil && !os.IsNotExist(readErr) {
			color.Error.Println(readErr)
		}

		if clientErr != nil && roblosCookie != "" {
			color.Error.Println(clientErr)
		}

		getCookie(c)
	}

	// Save cookie and API key to cookie.txt
	if err := saveCookieFile(c.Cookie, apiKey); err != nil {
		color.Error.Println("Failed to save cookie: ", err)
	}
	
	ensureAPIKey()

	fmt.Println("localhost started on port " + port + ". Waiting to start reuploading.")
	if err := serve(c); err != nil {
		log.Fatal(err)
	}
}

// parseCookieFile extracts the Roblox cookie and API key from the cookie file
func parseCookieFile(content string) (roblosCookie, apiKey string) {
	lines := strings.Split(strings.TrimSpace(content), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "api-key:") {
			apiKey = strings.TrimPrefix(line, "api-key:")
			apiKey = strings.TrimSpace(apiKey)
		} else if line != "" && !strings.HasPrefix(line, "api-key:") {
			// The first non-empty line that isn't an API key is the Roblox cookie
			roblosCookie = line
		}
	}
	return roblosCookie, apiKey
}

// saveCookieFile saves the Roblox cookie and API key to cookie.txt
func saveCookieFile(roblosCookie, apiKey string) error {
	var content strings.Builder
	content.WriteString(roblosCookie)
	content.WriteByte('\n')
	
	if apiKey != "" {
		content.WriteString("api-key:")
		content.WriteString(apiKey)
		content.WriteByte('\n')
	}
	
	return files.Write(cookieFile, content.String())
}

func getCookie(c *roblox.Client) {
	for {
		i, err := console.LongInput("ROBLOSECURITY: ")
		console.ClearScreen()
		if err != nil {
			color.Error.Println(err)
			continue
		}

		fmt.Println("Authenticating cookie...")
		err = c.SetCookie(i)
		console.ClearScreen()
		if err != nil {
			color.Error.Println(err)
			continue
		}

		break
	}
}

func ensureAPIKey() {
	currentAPIKey := strings.TrimSpace(config.Get("api_key"))
	if currentAPIKey != "" {
		return
	}

	fmt.Println("Enter your Open Cloud API key to enable mesh/animation uploads.")
	fmt.Println("How to get one:")
	fmt.Println("1. Go to https://create.roblox.com/dashboard/credentials?activeTab=ApiKeysTab")
	fmt.Println("2. Click Create API Key")
	fmt.Println("3. Enter any name")
	fmt.Println("4. Select Assets in Select API System")
	fmt.Println("5. Select Write in each Assets permission")
	key, err := console.Input("API key (leave blank to skip): ")
	if err != nil {
		color.Error.Println(err)
		return
	}

	key = strings.TrimSpace(key)
	if key == "" {
		return
	}

	config.Set("api_key", key)
	if err := config.Save(); err != nil {
		color.Error.Println("Failed to save api key: ", err)
	}
	
	// Also save API key to cookie.txt
	cookie, readErr := files.Read(cookieFile)
	if readErr != nil {
		color.Error.Println("Failed to read cookie file: ", readErr)
		return
	}
	
	roblosCookie, _ := parseCookieFile(cookie)
	if err := saveCookieFile(roblosCookie, key); err != nil {
		color.Error.Println("Failed to save API key to cookie.txt: ", err)
	}
}
