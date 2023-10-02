package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var client *mongo.Client // MongoDB 連線客戶端

// 連線到 MongoDB
func connectToMongoDB(mongodb_uri string) {
	// 設定 MongoDB 連線 URI
	uri := mongodb_uri

	// 設定 MongoDB 連線選項
	clientOptions := options.Client().ApplyURI(uri)

	// 連線到 MongoDB 伺服器
	var err error
	client, err = mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		fmt.Println("Failed to connect to MongoDB:", err)
		os.Exit(1)
	}

	// 確認與 MongoDB 伺服器的連線
	err = client.Ping(context.Background(), nil)
	if err != nil {
		fmt.Println("Failed to ping MongoDB:", err)
		os.Exit(1)
	}

	fmt.Println("Connected to MongoDB!")
}

// 關閉 MongoDB 連線
func closeMongoDBConnection() {
	if client != nil {
		err := client.Disconnect(context.Background())
		if err != nil {
			fmt.Println("Failed to disconnect from MongoDB:", err)
		} else {
			fmt.Println("Disconnected from MongoDB")
		}
	}
}

// 更新資料到 MongoDB
// func updateDataInMongoDB(bikeID string, columnName string, data interface{}) {
// 	// IBSMS.bikes
// 	bikeCollection := client.Database("IBSMS").Collection("bikes")

// 	// 查詢條件
// 	filter := bson.M{"bike_id": bikeID}

// 	// 更新資料
// 	update := bson.M{"$set": bson.M{columnName: data}}
// 	_, err := bikeCollection.UpdateOne(context.Background(), filter, update)
// 	if err != nil {
// 		fmt.Println("Failed to update data in MongoDB:", err)
// 		return
// 	}

// 	fmt.Println("Data updated in MongoDB!")
// }

// 插入單車的歷史數據到 MongoDB
func insertBikeHistoryData(bikeID string, timeObj time.Time, tire_pressure []float64, seat_rotate []float64, isBrake bool, seat_tube []float64, keyes_pressure int64) {
	// 使用 bikes_history 集合
	historyCollection := client.Database("IBSMS").Collection("bikes_history")

	// 數據
	data := bson.M{
		"bikeId":         bikeID,
		"timestamp":      timeObj,
		"tire_pressure":  tire_pressure,
		"seat_rotate":    seat_rotate,
		"isBrake":        isBrake,
		"seat_tube":      seat_tube,
		"keyes_pressure": keyes_pressure,
	}

	_, err := historyCollection.InsertOne(context.Background(), data)
	if err != nil {
		fmt.Println("Failed to insert data into MongoDB:", err)
		return
	}

	fmt.Println("Data inserted into MongoDB!")
}

// 建立 MQTT 用戶端
func createMQTTClient(brokerURL, clientID, username, password string) mqtt.Client {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(brokerURL)
	opts.SetClientID(clientID)
	opts.SetUsername(username)                    // 設定用戶名
	opts.SetPassword(password)                    // 設定密碼
	opts.SetDefaultPublishHandler(messageHandler) // 設置默認的訊息處理器
	opts.OnConnect = func(c mqtt.Client) {
		fmt.Println("Connected to MQTT Broker!")
	}

	opts.OnConnectionLost = func(c mqtt.Client, err error) {
		fmt.Printf("Connection lost: %s\n", err)
	}
	client := mqtt.NewClient(opts)
	return client
}

// MQTT 訊息處理器
func messageHandler(client mqtt.Client, msg mqtt.Message) {
	fmt.Printf("Received message on topic: %s\n", msg.Topic())
	fmt.Printf("Message: %s\n", msg.Payload())

	var payload map[string]interface{}
	err := json.Unmarshal(msg.Payload(), &payload)
	if err != nil {
		fmt.Println("Failed to parse payload:", err)
		return
	}

	// 忽略歡迎訊息
	if _, ok := payload["msg"]; ok {
		fmt.Println("Ignore welcome message")
		return
	}

	// bikeId, ok := payload["bike_id"].(string)
	// if !ok {
	// 	fmt.Println("Invalid bike_id")
	// 	return
	// }

	// columnName, ok := payload["column_name"].(string)
	// if !ok {
	// 	fmt.Println("Invalid column_name")
	// 	return
	// }

	// data, ok := payload["data"]
	// if !ok {
	// 	fmt.Println("Invalid data")
	// 	return
	// }

	// 假设您的 payload 包含所需的所有字段
	bikeId := payload["bike_id"].(string)
	timestampFloat := payload["timestamp"].(float64)
	timestampInt64 := int64(timestampFloat)
	timeObj := time.Unix(timestampInt64, 0)
	tire_pressure := payload["tire"].([]interface{})
	tire_pressure_floats := make([]float64, len(tire_pressure))
	for i, v := range tire_pressure {
		tire_pressure_floats[i] = v.(float64)
	}

	seat_rotate_interface, ok := payload["seat_rotate"].([]interface{})
	if !ok || seat_rotate_interface == nil {
		fmt.Println("Invalid or nil seat_rotate")
		return
	}

	seat_rotate := make([]float64, len(seat_rotate_interface))
	for i, v := range seat_rotate_interface {
		if val, ok := v.(float64); ok {
			seat_rotate[i] = val
		} else {
			fmt.Println("Invalid value in seat_rotate slice")
			return
		}
	}

	isBrake := payload["isBrake"].(bool)
	seat_tube := payload["acceleration"].([]interface{})
	seat_tube_floats := make([]float64, len(seat_tube))
	for i, v := range seat_tube {
		seat_tube_floats[i] = v.(float64)
	}

	keyes_pressure := payload["seat_tube"].(float64)
	keyes_pressure_int := int64(keyes_pressure)

	insertBikeHistoryData(bikeId, timeObj, tire_pressure_floats, seat_rotate, isBrake, seat_tube_floats, keyes_pressure_int)

	// updateDataInMongoDB(bikeID, columnName, data)
}

// 訂閱主題
func subscribe(client mqtt.Client, topic string) {
	token := client.Subscribe(topic, 1, nil)
	token.Wait()
	if token.Error() != nil {
		fmt.Printf("Error subscribing to topic: %s\n", token.Error())
		os.Exit(1)
	}
	fmt.Printf("Subscribed to topic: %s\n", topic)
}

// 產生隨機的clientID
func generateRandomClientID() string {
	return fmt.Sprintf("mqtt-ibsms-%d", time.Now().UnixNano())
}

type Config struct {
	Mongodb_uri string `json:"mongodb_uri"`
	Username    string `json:"username"`
	Password    string `json:"password"`
}

func main() {
	file, err := os.Open("config.json")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	var acc Config

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&acc)
	if err != nil {
		fmt.Println("Error decoding JSON:", err)
		return
	}
	brokerURL := "mqtt://139.99.89.162:1883" // MQTT Broker的位址
	clientID := generateRandomClientID()     // 用戶端ID
	topic := "bike_data"                     // 訂閱的主題
	fmt.Println("ClientID:", clientID)

	connectToMongoDB(acc.Mongodb_uri) // 連線到 MongoDB

	client := createMQTTClient(brokerURL, clientID, acc.Username, acc.Password)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		fmt.Printf("Error connecting to MQTT Broker: %s\n", token.Error())
		os.Exit(1)
	}

	for !client.IsConnected() {
		time.Sleep(100 * time.Millisecond)
	}

	subscribe(client, topic)

	// 等待中斷信號
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	// 斷開連線
	client.Disconnect(250)

	closeMongoDBConnection() // 關閉 MongoDB 連線
}
