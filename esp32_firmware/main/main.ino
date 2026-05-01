#include <WiFi.h>
#include <PubSubClient.h>
#include <DHT.h>
#include <ArduinoJson.h>

const char* WIFI_SSID     = "Gujjus";
const char* WIFI_PASSWORD = "rowanA220";
const char* MQTT_BROKER   = "10.0.0.18";
const int   MQTT_PORT     = 1883;
const char* MQTT_TOPIC    = "airguard/sensors/raw";
const char* DEVICE_ID     = "ESP32_AIRGUARD_01";
const char* LOCATION      = "Lab_Room_204";

#define MQ135_PIN  34
#define MQ7_PIN    35
#define DHT_PIN    4
#define DHT_TYPE   DHT22

DHT dht(DHT_PIN, DHT_TYPE);
WiFiClient espClient;
PubSubClient mqttClient(espClient);

unsigned long lastPublish = 0;
const long PUBLISH_INTERVAL = 2000;
int messageCount = 0;

float calculateAQI(float mq135) {
  if (mq135 <= 400) return (mq135 - 300) * 50.0 / 100.0;
  if (mq135 <= 500) return 51 + (mq135 - 400) * 49.0 / 100.0;
  if (mq135 <= 600) return 101 + (mq135 - 500) * 49.0 / 100.0;
  if (mq135 <= 750) return 151 + (mq135 - 600) * 49.0 / 150.0;
  return 201;
}

void connectWiFi() {
  Serial.print("Connecting to WiFi: ");
  Serial.println(WIFI_SSID);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  int attempts = 0;
  while (WiFi.status() != WL_CONNECTED && attempts < 20) {
    delay(500);
    Serial.print(".");
    attempts++;
  }
  if (WiFi.status() == WL_CONNECTED) {
    Serial.println("\nWiFi connected!");
    Serial.print("IP: ");
    Serial.println(WiFi.localIP());
  } else {
    Serial.println("\nWiFi FAILED!");
  }
}

void connectMQTT() {
  mqttClient.setServer(MQTT_BROKER, MQTT_PORT);
  int attempts = 0;
  while (!mqttClient.connected() && attempts < 5) {
    Serial.print("Connecting to MQTT...");
    String clientId = "ESP32_" + String(random(0xffff), HEX);
    if (mqttClient.connect(clientId.c_str())) {
      Serial.println("connected!");
    } else {
      Serial.print("failed rc=");
      Serial.println(mqttClient.state());
      delay(3000);
      attempts++;
    }
  }
}

float readMQSensor(int pin) {
  int raw = analogRead(pin);
  float voltage = (raw / 4095.0) * 3.3;
  float ppm = voltage * 200.0;
  return ppm;
}

String generateMsgId() {
  String id = "";
  for (int i = 0; i < 16; i++) {
    id += String(random(16), HEX);
  }
  return id;
}

void setup() {
  Serial.begin(115200);
  delay(1000);
  Serial.println("=== AirGuard ESP32 Starting ===");
  dht.begin();
  pinMode(DHT_PIN, INPUT_PULLUP);
  delay(2000);
  connectWiFi();
  connectMQTT();
  Serial.println("=== Ready ===");
}

void loop() {
  if (!mqttClient.connected()) {
    connectMQTT();
  }
  mqttClient.loop();

  unsigned long now = millis();
  if (now - lastPublish >= PUBLISH_INTERVAL) {
    lastPublish = now;

    float mq135 = readMQSensor(MQ135_PIN);
    float mq7   = readMQSensor(MQ7_PIN);
    float temp  = dht.readTemperature();
    float hum   = dht.readHumidity();

    if (isnan(temp) || isnan(hum)) {
      Serial.println("DHT22 read failed!");
      temp = 0;
      hum  = 0;
    }

    float aqi = calculateAQI(mq135);

    JsonDocument doc;
    doc["device_id"]     = DEVICE_ID;
    doc["location"]      = LOCATION;
    doc["timestamp"]     = millis();
    doc["mq135_ppm"]     = round(mq135 * 100) / 100.0;
    doc["mq7_ppm"]       = round(mq7 * 100) / 100.0;
    doc["temperature_c"] = round(temp * 100) / 100.0;
    doc["humidity_pct"]  = round(hum * 100) / 100.0;
    doc["aqi_value"]     = round(aqi * 10) / 10.0;
    doc["scenario"]      = "normal";
    doc["msg_id"]        = generateMsgId();

    char payload[256];
    serializeJson(doc, payload);

    if (mqttClient.publish(MQTT_TOPIC, payload)) {
      messageCount++;
      Serial.print("Published #");
      Serial.print(messageCount);
      Serial.print(" | AQI:");
      Serial.print(aqi);
      Serial.print(" | Temp:");
      Serial.print(temp);
      Serial.print("C | Hum:");
      Serial.print(hum);
      Serial.println("%");
    } else {
      Serial.println("Publish failed!");
    }
  }
}