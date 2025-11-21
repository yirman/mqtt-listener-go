package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
)

const (
	receiveTopic = "suba_driver/data"      // topic to subscribe and receive LatLon
	forwardTopic = "suba_driver/passenger" // topic to forward received messages to
)

// buffered channel to avoid blocking the MQTT network callback
var mqttMsgChan = make(chan mqtt.Message, 200)

var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	// avoid forwarding messages that were already forwarded
	if msg.Topic() != forwardTopic {
		// copy payload for asynchronous publish
		payload := append([]byte(nil), msg.Payload()...)
		go func(p []byte) {
			t := client.Publish(forwardTopic, 1, false, p)
			t.Wait()
			if t.Error() != nil {
				fmt.Printf("Error forwarding message to %s: %v\n", forwardTopic, t.Error())
			}
		}(payload)
	}

	// non-blocking send to processor: drop when busy
	select {
	case mqttMsgChan <- msg:
	default:
		fmt.Println("Dropping message: processor busy")
	}
}

type LatLon struct {
	Latitude  float64         `json:"latitude"`
	Longitude float64         `json:"longitude"`
	Speed     json.RawMessage `json:"speed,omitempty"`
}

func processMsg(ctx context.Context, logger *zerolog.Logger, input <-chan mqtt.Message) chan LatLon {
	out := make(chan LatLon)
	go func() {
		defer close(out)
		for {
			select {
			case msg, ok := <-input:
				if !ok {
					return
				}
				fmt.Printf("Received message: %s from topic: %s\n", msg.Payload(), msg.Topic())
				var ll LatLon
				err := json.Unmarshal(msg.Payload(), &ll)
				if err != nil {
					logger.Error().Err(err).Msg("Error unmarshalling LatLon message")
				} else {
					out <- ll
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	fmt.Println("Connected to MQTT Broker")
	// (re)subscribe on connect so subscriptions survive reconnects
	if token := client.Subscribe(receiveTopic, 1, nil); token.Wait() && token.Error() != nil {
		fmt.Printf("Resubscribe failed for %s: %v\n", receiveTopic, token.Error())
	} else {
		fmt.Printf("Subscribed to topic: %s\n", receiveTopic)
	}
}

var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	fmt.Printf("Connection lost: %v", err)
}

func main() {
	appCtx := context.Background()
	logger := setupLogger(appCtx, "")

	// not persisting to Postgres for now — just print received messages

	opts := mqtt.NewClientOptions()
	opts.AddBroker(os.Getenv("MQTT_BROKER"))
	opts.SetClientID(os.Getenv("CLIENT_ID"))
	opts.SetUsername(os.Getenv("CLIENT_USER_NAME"))
	opts.SetPassword("")       // leave blank
	opts.SetProtocolVersion(4) // MQTT v3.1.1
	opts.SetDefaultPublishHandler(messagePubHandler)
	opts.OnConnect = connectHandler
	opts.OnConnectionLost = connectLostHandler

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	ctx, cancel := context.WithCancel(appCtx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		processedChan := processMsg(ctx, logger, mqttMsgChan)
		for ll := range processedChan {
			// just print the processed IoT message — no DB persistence
			// convert speed to string for logging and forwarding
			speedStr := parseSpeedToString(ll.Speed)
			logger.Info().Msg(fmt.Sprintf("Received LatLon: %+v speed=%s", ll, speedStr))

			// call Google Directions API and extract needed fields
			apiKey := os.Getenv("GOOGLE_API_KEY")
			// pass the current message latitude/longitude as the origin
			dirs, err := fetchDirections(ctx, apiKey, ll.Latitude, ll.Longitude)
			if err != nil {
				logger.Error().Err(err).Msg("Error fetching directions")
				continue
			}

			// log extracted fields: routes.bounds and legs distance/duration_in_traffic
			for i, r := range dirs.Routes {
				logger.Info().Msgf("Route[%d] bounds: NE(%f,%f) SW(%f,%f)", i,
					r.Bounds.Northeast.Lat, r.Bounds.Northeast.Lng,
					r.Bounds.Southwest.Lat, r.Bounds.Southwest.Lng)
				for j, leg := range r.Legs {
					logger.Info().Msgf("Route[%d] Leg[%d] distance: %s (%d)", i, j, leg.Distance.Text, leg.Distance.Value)
					logger.Info().Msgf("Route[%d] Leg[%d] duration_in_traffic: %s (%d)", i, j, leg.DurationInTraffic.Text, leg.DurationInTraffic.Value)
				}
			}

			// Build a message matching `receiver_json` structure and publish to forwardTopic
			var out struct {
				Lat               float64 `json:"lat"`
				Lon               float64 `json:"lon"`
				Speed             string  `json:"speed"`
				Bounds            any     `json:"bounds"`
				Distance          any     `json:"distance"`
				Duration          any     `json:"duration"`
				DurationInTraffic any     `json:"duration_in_traffic"`
			}
			out.Lat = ll.Latitude
			out.Lon = ll.Longitude

			// set speed string from incoming raw value
			out.Speed = parseSpeedToString(ll.Speed)

			// default empty values
			out.Bounds = map[string]any{}
			out.Distance = map[string]any{"text": "", "value": 0}
			out.Duration = map[string]any{"text": "", "value": 0}
			out.DurationInTraffic = map[string]any{"text": "", "value": 0}

			if len(dirs.Routes) > 0 {
				r := dirs.Routes[0]
				out.Bounds = map[string]any{
					"northeast": map[string]float64{"lat": r.Bounds.Northeast.Lat, "lng": r.Bounds.Northeast.Lng},
					"southwest": map[string]float64{"lat": r.Bounds.Southwest.Lat, "lng": r.Bounds.Southwest.Lng},
				}
				if len(r.Legs) > 0 {
					leg := r.Legs[0]
					out.Distance = map[string]any{"text": leg.Distance.Text, "value": leg.Distance.Value}
					out.Duration = map[string]any{"text": leg.Duration.Text, "value": leg.Duration.Value}
					out.DurationInTraffic = map[string]any{"text": leg.DurationInTraffic.Text, "value": leg.DurationInTraffic.Value}
				}
			}

			payload, err := json.Marshal(out)
			if err != nil {
				logger.Error().Err(err).Msg("Error marshalling forwarded receiver_json payload")
				continue
			}

			// publish asynchronously so we don't block processing
			p := append([]byte(nil), payload...)
			go func(data []byte) {
				t := client.Publish(forwardTopic, 1, false, data)
				t.Wait()
				if t.Error() != nil {
					fmt.Printf("Error publishing processed data to %s: %v\n", forwardTopic, t.Error())
				}
			}(p)
		}
	}()

	// Subscribe to the receive topic (also handled in OnConnect)
	token := client.Subscribe(receiveTopic, 1, nil)
	token.Wait()
	if token.Error() != nil {
		fmt.Printf("Subscribe failed for %s: %v\n", receiveTopic, token.Error())
	} else {
		fmt.Printf("Subscribed to topic: %s\n", receiveTopic)
	}

	// Wait for interrupt signal to gracefully shutdown the subscriber
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	// Unsubscribe and disconnect first to stop incoming messages,
	// then cancel processing goroutines and wait for them to finish.
	fmt.Println("Unsubscribing and disconnecting...")
	if t := client.Unsubscribe(receiveTopic); t != nil {
		t.Wait()
	}
	client.Disconnect(250)

	// Cancel the context to signal the goroutine to stop
	cancel()

	// Wait for the goroutine to finish
	wg.Wait()
	// not using a DB connection to close
	fmt.Println("Goroutine terminated, exiting...")
}

func setupLogger(ctx context.Context, logFilePath string) *zerolog.Logger {
	var outWriter = os.Stdout
	if logFilePath != "" && logFilePath != "stdout" {
		file, err := os.OpenFile(logFilePath,
			os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0664)
		if err != nil {
			log.Fatalln(err)
		}
		outWriter = file
	}
	cout := zerolog.ConsoleWriter{Out: outWriter, TimeFormat: time.RFC822}
	cout.FormatLevel = func(i interface{}) string {
		return strings.ToUpper(fmt.Sprintf("| %-6s|", i))
	}
	// uncomment to remove timestamp from logs
	//out.FormatTimestamp = func(i interface{}) string {
	//	return ""
	//}
	baseLogger := zerolog.New(cout).With().Timestamp().Logger()
	logCtx := baseLogger.WithContext(ctx)
	l := zerolog.Ctx(logCtx)
	return l
}

// parseSpeedToString converts a json.RawMessage that may contain a string or number into a string value.
func parseSpeedToString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	// try string
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	// try number (float)
	var f float64
	if err := json.Unmarshal(raw, &f); err == nil {
		// format without trailing .0 when integer
		if f == float64(int64(f)) {
			return fmt.Sprintf("%d", int64(f))
		}
		return fmt.Sprintf("%v", f)
	}
	// fallback: return raw as string
	return string(raw)
}

// DirectionsResponse contains only the fields we need from Google's Directions API
type DirectionsResponse struct {
	Routes []struct {
		Bounds struct {
			Northeast struct {
				Lat float64 `json:"lat"`
				Lng float64 `json:"lng"`
			} `json:"northeast"`
			Southwest struct {
				Lat float64 `json:"lat"`
				Lng float64 `json:"lng"`
			} `json:"southwest"`
		} `json:"bounds"`
		Legs []struct {
			Distance struct {
				Text  string `json:"text"`
				Value int    `json:"value"`
			} `json:"distance"`

			Duration struct {
				Text  string `json:"text"`
				Value int    `json:"value"`
			} `json:"duration"`
			DurationInTraffic struct {
				Text  string `json:"text"`
				Value int    `json:"value"`
			} `json:"duration_in_traffic"`
		} `json:"legs"`
	} `json:"routes"`
}

// fetchDirections calls the Google Directions API for a fixed origin/destination
func fetchDirections(ctx context.Context, apiKey string, originLat, originLng float64) (*DirectionsResponse, error) {
	// Build URL using provided origin latitude/longitude and static destination
	url := fmt.Sprintf("https://maps.googleapis.com/maps/api/directions/json?origin=%f,%f&destination=8.2714064,-62.737805&mode=driving&departure_time=now", originLat, originLng)
	if apiKey != "" {
		url = url + "&key=" + apiKey
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 8 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("directions API returned status %d: %s", resp.StatusCode, string(b))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var dr DirectionsResponse
	if err := json.Unmarshal(body, &dr); err != nil {
		return nil, err
	}
	return &dr, nil
}
