#include <WiFi.h>
#include <PubSubClient.h>
#include <DHT.h>
#include <ArduinoJson.h>

// ── WiFi & MQTT Settings ───────────────────────────────────────────────
const char* WIFI_SSID     = "YOUR_WIFI_NAME";
const char* WIFI_PASSWORD = "YOUR_WIFI_PASSWORD";
const char* MQTT_BROKER   = "YOUR_PC_IP_ADDRESS";
const int   MQTT_PORT     = 1883;
const char* MQTT_TOPIC    = "airguard/sensors/raw";
const char* DEVICE_ID     = "ESP32_AIRGUARD_01";
const char* LOCATION      = "Lab_Room_204";

// ── Pin Definitions ────────────────────────────────────────────────────
#define MQ135_PIN    34
#define MQ7_PIN      35
#define DHT_PIN      4
#define DHT_TYPE     DHT22

// ── Objects ────────────────────────────────────────────────────────────
DHT dht(DHT_PIN, DHT_TYPE);
WiFiClient espClient;
PubSubClient mqttClient(espClient);

// ── Timing ─────────────────────────────────────────────────────────────
unsigned long lastPublish = 0;
const long PUBLISH_INTERVAL = 2000;
int messageCount = 0;

// ── WiFi Connect ───────────────────────────────────────────────────────
void connectWiFi() {
  Serial.print("Connecting to WiFi: ");
  Serial.println(WIFI_SSID);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }
  Serial.println("\nWiFi connected!");
  Serial.print("IP Address: ");
  Serial.println(WiFi.localIP());
}

// ── MQTT Connect ───────────────────────────────────────────────────────
void connectMQTT() {
  mqttClient.setServer(MQTT_BROKER, MQTT_PORT);
  while (!mqttClient.connected()) {
    Serial.print("Connecting to MQTT broker...");
    String clientId = "ESP32_" + String(random(0xffff), HEX);
    if (mqttClient.connect(clientId.c_str())) {
      Serial.println("connected!");
    } else {
      Serial.print("failed, rc=");
      Serial.print(mqttClient.state());
      Serial.println(" retrying in 5s");
      delay(5000);
    }
  }
}

// ── Read MQ Sensor ─────────────────────────────────────────────────────
float readMQSensor(int pin) {
  int raw = analogRead(pin);
  // Convert ADC reading to ppm (simplified linear mapping)
  // ESP32 ADC: 0-4095 mapped to sensor range
  float voltage = (raw / 4095.0) * 3.3;
  float ppm = voltage * 200.0;
  return ppm;
}

// ── Generate Message ID ────────────────────────────────────────────────
String generateMsgId() {
  String id = "";
  for (int i = 0; i < 16; i++) {
    id += String(random(16), HEX);
  }
  return id;
}

// ── Setup ──────────────────────────────────────────────────────────────
void setup() {
  Serial.begin(115200);
  delay(1000);
  Serial.println("=== AirGuard ESP32 Starting ===");

  dht.begin();
  delay(2000);

  connectWiFi();
  connectMQTT();

  Serial.println("=== Ready — publishing sensor data ===");
}

// ── Loop ───────────────────────────────────────────────────────────────
void loop() {
  if (!mqttClient.connected()) {
    connectMQTT();
  }
  mqttClient.loop();

  unsigned long now = millis();
  if (now - lastPublish >= PUBLISH_INTERVAL) {
    lastPublish = now;

    // Read sensors
    float mq135 = readMQSensor(MQ135_PIN);
    float mq7   = readMQSensor(MQ7_PIN);
    float temp  = dht.readTemperature();
    float hum   = dht.readHumidity();

    // Check DHT22 reading
    if (isnan(temp) || isnan(hum)) {
      Serial.println("DHT22 read failed — skipping");
      return;
    }

    // Build JSON payload
    StaticJsonDocument<256> doc;
    doc["device_id"]     = DEVICE_ID;
    doc["location"]      = LOCATION;
    doc["timestamp"]     = millis();
    doc["mq135_ppm"]     = round(mq135 * 100) / 100.0;
    doc["mq7_ppm"]       = round(mq7 * 100) / 100.0;
    doc["temperature_c"] = round(temp * 100) / 100.0;
    doc["humidity_pct"]  = round(hum * 100) / 100.0;
    doc["scenario"]      = "normal";
    doc["msg_id"]        = generateMsgId();

    char payload[256];
    serializeJson(doc, payload);

    // Publish to MQTT
    if (mqttClient.publish(MQTT_TOPIC, payload)) {
      messageCount++;
      Serial.print("Published #");
      Serial.print(messageCount);
      Serial.print(" | MQ135: ");
      Serial.print(mq135);
      Serial.print(" | MQ7: ");
      Serial.print(mq7);
      Serial.print(" | Temp: ");
      Serial.print(temp);
      Serial.print("C | Hum: ");
      Serial.print(hum);
      Serial.println("%");
    } else {
      Serial.println("Publish failed!");
    }
  }
}