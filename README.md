# Akıllı Mama Kabı — Web Kontrol Paneli

NodeMCU (ESP8266) tabanlı akıllı hayvan mama kabını internet üzerinden uzaktan kontrol eden Streamlit web uygulaması.

## Yerelde Çalıştırma

1. Bağımlılıkları kurun:
   ```bash
   pip install -r requirements.txt
   ```
2. `.streamlit/secrets.toml.example` dosyasını `.streamlit/secrets.toml` olarak kopyalayıp kendi ayarlarınızı girin:
   ```toml
   MQTT_BROKER = "broker.hivemq.com"
   MQTT_PORT = 1883
   MQTT_TOPIC_PREFIX = "ornek-topic-oneki"
   APP_PASSWORD = "sifre"
   ```
3. Uygulamayı başlatın:
   ```bash
   python -m streamlit run app.py
   ```

## Streamlit Community Cloud Deploy

Repo'yu Streamlit Community Cloud'a bağladıktan sonra **App Settings > Secrets** bölümüne `secrets.toml` içeriğinizi yapıştırmanız yeterlidir.
