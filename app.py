import hmac
import json
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

import paho.mqtt.client as mqtt
import streamlit as st

TR_TZ = timezone(timedelta(hours=3))


def simdi_saat() -> str:
    return datetime.now(TR_TZ).strftime("%H:%M:%S")


def load_secrets() -> tuple[str, int, str, str]:
    """Gizli ayarları st.secrets içinden güvenli şekilde okur."""
    try:
        broker = str(st.secrets.get("MQTT_BROKER", "broker.hivemq.com")).strip()
        port = int(st.secrets.get("MQTT_PORT", 1883))
        topic_prefix = str(st.secrets["MQTT_TOPIC_PREFIX"]).strip().strip("/")
        app_password = str(st.secrets["APP_PASSWORD"])
        if not topic_prefix or not app_password:
            raise KeyError("Bos secret degeri")
        return broker, port, topic_prefix, app_password
    except Exception:
        st.error("Uygulama ayarları eksik. Lütfen yöneticinizle iletişime geçin.")
        st.stop()


class MqttFeederManager:
    """Arka planda tek bir kalıcı MQTT bağlantısı tutan thread-safe yönetici."""

    def __init__(self, broker: str, port: int, topic_prefix: str) -> None:
        self._lock = threading.Lock()
        self.broker = broker
        self.port = port
        self.topic_komut = f"{topic_prefix}/komut"
        self.topic_durum = f"{topic_prefix}/durum"
        self.topic_online = f"{topic_prefix}/online"

        self.broker_connected: bool = False
        self.device_online: str = "unknown"  # "online" | "offline" | "unknown"
        self.kapak: str = "kapali"  # "acik" | "kapali"
        self.aci: int = 0
        self.last_updated: str = "-"

        client_id = f"streamlit-kap-{uuid.uuid4().hex[:8]}"
        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            protocol=mqtt.MQTTv311,
        )
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        self.client.reconnect_delay_set(min_delay=1, max_delay=10)

        try:
            self.client.connect_async(broker, port, keepalive=30)
            self.client.loop_start()
        except Exception:
            pass

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        ok = not reason_code.is_failure
        with self._lock:
            self.broker_connected = ok

        if ok:
            client.subscribe(self.topic_durum)
            client.subscribe(self.topic_online)

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        with self._lock:
            self.broker_connected = False

    def _on_message(self, client, userdata, msg):
        payload = msg.payload.decode("utf-8", errors="ignore").strip()
        with self._lock:
            if msg.topic == self.topic_online:
                val = payload.lower()
                if val in ("online", "offline"):
                    self.device_online = val
                return

            if msg.topic == self.topic_durum:
                try:
                    data = json.loads(payload)
                    kapak = data.get("kapak")
                    if kapak in ("acik", "kapali"):
                        self.kapak = kapak
                        aci = data.get("aci")
                        self.aci = (
                            int(aci)
                            if isinstance(aci, (int, float))
                            else (180 if kapak == "acik" else 0)
                        )
                        self.last_updated = simdi_saat()
                except Exception:
                    val = payload.lower()
                    if val in ("acik", "kapali"):
                        self.kapak = val
                        self.aci = 180 if val == "acik" else 0
                        self.last_updated = simdi_saat()

    def send_command(self, command: str) -> bool:
        with self._lock:
            connected = self.broker_connected
        if not connected:
            return False

        info = self.client.publish(self.topic_komut, command, qos=0, retain=False)
        ok = info.rc == mqtt.MQTT_ERR_SUCCESS
        if ok:
            with self._lock:
                self.last_updated = simdi_saat()
        return ok

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "broker_connected": self.broker_connected,
                "device_online": self.device_online,
                "kapak": self.kapak,
                "aci": self.aci,
                "last_updated": self.last_updated,
            }


@st.cache_resource
def get_mqtt_manager(broker: str, port: int, topic_prefix: str) -> MqttFeederManager:
    manager = MqttFeederManager(broker, port, topic_prefix)
    deadline = time.time() + 2.0
    while not manager.broker_connected and time.time() < deadline:
        time.sleep(0.1)
    time.sleep(0.25)
    return manager


# --- Sayfa Yapılandırması ve Hem Light Hem Dark Uyumlu Stil ---
st.set_page_config(
    page_title="Akıllı Mama Kabı",
    page_icon="🐾",
    layout="centered",
)

st.markdown(
    """
    <style>
    /* Şifre kutusunu HTML type="text" tutup görsel olarak maskele (1Password / şifre yöneticilerini engeller) */
    input[aria-label="Erişim Kodu"] {
        -webkit-text-security: disc !important;
        text-security: disc !important;
    }
    .header-box {
        display: flex;
        align-items: center;
        gap: 12px;
        margin-bottom: 6px;
    }
    .logo-badge {
        width: 46px;
        height: 46px;
        border-radius: 14px;
        background: rgba(16, 185, 129, 0.14);
        border: 1px solid rgba(16, 185, 129, 0.35);
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 24px;
    }
    .status-strip {
        background: var(--secondary-background-color);
        border: 1px solid rgba(148, 163, 184, 0.25);
        border-radius: 16px;
        padding: 12px 14px;
        display: flex;
        align-items: center;
        flex-wrap: wrap;
        gap: 8px;
    }
    .status-pill {
        display: inline-flex;
        align-items: center;
        gap: 7px;
        padding: 6px 12px;
        border-radius: 999px;
        font-size: 13px;
        font-weight: 700;
        line-height: 1.2;
    }
    .pill-green {
        background: rgba(16, 185, 129, 0.14);
        border: 1px solid rgba(16, 185, 129, 0.35);
        color: #10B981;
    }
    .pill-amber {
        background: rgba(245, 158, 11, 0.14);
        border: 1px solid rgba(245, 158, 11, 0.35);
        color: #F59E0B;
    }
    .pill-red {
        background: rgba(239, 68, 68, 0.14);
        border: 1px solid rgba(239, 68, 68, 0.35);
        color: #EF4444;
    }
    .pill-dot {
        width: 8px;
        height: 8px;
        border-radius: 999px;
        display: inline-block;
    }
    .status-card {
        background: var(--secondary-background-color);
        border-radius: 24px;
        padding: 26px 20px;
        text-align: center;
        margin: 14px 0 18px 0;
    }
    .status-open {
        border: 2px solid rgba(16, 185, 129, 0.55);
    }
    .status-closed {
        border: 2px solid rgba(245, 158, 11, 0.5);
    }
    .circle-badge {
        width: 116px;
        height: 116px;
        border-radius: 58px;
        margin: 0 auto 14px auto;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 46px;
    }
    .circle-open {
        background: rgba(16, 185, 129, 0.14);
        border: 3px solid #10B981;
    }
    .circle-closed {
        background: rgba(245, 158, 11, 0.14);
        border: 3px solid #F59E0B;
    }
    .metrics-bar {
        display: flex;
        justify-content: space-around;
        background: var(--background-color);
        border: 1px solid rgba(148, 163, 184, 0.2);
        border-radius: 12px;
        padding: 10px 14px;
        margin-top: 16px;
        font-size: 13px;
        color: var(--text-color);
    }
    .muted-text {
        color: var(--text-color);
        opacity: 0.72;
    }
    .loading-card {
        background: var(--secondary-background-color);
        border: 1px solid rgba(16, 185, 129, 0.3);
        border-radius: 22px;
        padding: 42px 24px;
        text-align: center;
        margin-top: 48px;
    }
    .spinner-ring {
        width: 44px;
        height: 44px;
        border: 4px solid rgba(16, 185, 129, 0.2);
        border-top-color: #10B981;
        border-radius: 50%;
        margin: 18px auto 12px auto;
        animation: kapspin 0.85s linear infinite;
    }
    @keyframes kapspin {
        to { transform: rotate(360deg); }
    }
    div.stButton > button {
        border-radius: 16px;
        padding: 0.78rem 1rem;
        font-weight: 700;
        font-size: 15px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

MQTT_BROKER, MQTT_PORT, TOPIC_PREFIX, APP_PASSWORD = load_secrets()

# --- Oturum Durumları ---
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
if "is_loading_panel" not in st.session_state:
    st.session_state.is_loading_panel = False
if "login_error" not in st.session_state:
    st.session_state.login_error = False


def try_login() -> None:
    entered = st.session_state.get("access_code_input", "")
    if not entered:
        return
    if hmac.compare_digest(entered, APP_PASSWORD):
        st.session_state.authenticated = True
        st.session_state.is_loading_panel = True
        st.session_state.login_error = False
        st.session_state.access_code_input = ""
    else:
        st.session_state.login_error = True


# --- 1. Basit Giriş Ekranı (Şifre yöneticisi tetiklemeyen yapı) ---
if not st.session_state.authenticated:
    st.markdown("### 🐾 Giriş")

    st.text_input(
        "Erişim Kodu",
        key="access_code_input",
        type="default",
        autocomplete="off",
        placeholder="Şifreyi giriniz...",
        on_change=try_login,
    )

    if st.button("Giriş Yap", use_container_width=True, type="primary"):
        try_login()
        if st.session_state.authenticated:
            st.rerun()

    if st.session_state.login_error:
        st.error("Hatalı şifre.")

    st.stop()


# --- 2. Giriş Sonrası Temiz Yüklenme (Loading) Ekranı ---
if st.session_state.is_loading_panel:
    placeholder = st.empty()
    placeholder.markdown(
        """
        <div class="loading-card">
            <div style="font-size:44px;">🐾</div>
            <div class="spinner-ring"></div>
            <div style="font-size:18px; font-weight:700; margin-top:8px;">
                Kontrol Paneli Hazırlanıyor
            </div>
            <div class="muted-text" style="font-size:13px; margin-top:6px;">
                İletişim sunucusuna bağlanılıyor ve cihaz durumu alınıyor...
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    # Bağlantıyı kur ve kısa yumuşak geçiş sağla
    mgr = get_mqtt_manager(MQTT_BROKER, MQTT_PORT, TOPIC_PREFIX)
    mgr.send_command("durum")
    time.sleep(0.55)
    st.session_state.is_loading_panel = False
    placeholder.empty()
    st.rerun()


# --- 3. Ana Kontrol Paneli ---
manager = get_mqtt_manager(MQTT_BROKER, MQTT_PORT, TOPIC_PREFIX)

header_col, logout_col = st.columns([4, 1])
with header_col:
    st.markdown(
        """
        <div class="header-box">
            <div class="logo-badge">🐾</div>
            <h2 style="margin:0;">Akıllı Mama Kabı</h2>
        </div>
        """,
        unsafe_allow_html=True,
    )
with logout_col:
    st.write("")
    if st.button("🚪 Çıkış", use_container_width=True):
        st.session_state.authenticated = False
        st.session_state.is_loading_panel = False
        st.rerun()


@st.fragment(run_every="2s")
def render_live_status():
    state = manager.snapshot()
    is_open = state["kapak"] == "acik"

    if state["broker_connected"]:
        server_pill = (
            '<span class="status-pill pill-green">'
            '<span class="pill-dot" style="background:#10B981;"></span>'
            'İletişim sunucusu bağlı'
            '</span>'
        )
    else:
        server_pill = (
            '<span class="status-pill pill-amber">'
            '<span class="pill-dot" style="background:#F59E0B;"></span>'
            'Sunucuya bağlanılıyor...'
            '</span>'
        )

    if state["device_online"] == "online":
        device_pill = (
            '<span class="status-pill pill-green">'
            '<span class="pill-dot" style="background:#10B981;"></span>'
            'Cihaz Çevrimiçi'
            '</span>'
        )
    elif state["device_online"] == "offline":
        device_pill = (
            '<span class="status-pill pill-red">'
            '<span class="pill-dot" style="background:#EF4444;"></span>'
            'Cihaz Çevrimdışı'
            '</span>'
        )
    else:
        device_pill = (
            '<span class="status-pill pill-amber">'
            '<span class="pill-dot" style="background:#F59E0B;"></span>'
            'Cihaz Bekleniyor'
            '</span>'
        )

    strip_col, refresh_col = st.columns([4, 1], vertical_alignment="center")
    with strip_col:
        st.markdown(
            f'<div class="status-strip">{server_pill}{device_pill}</div>',
            unsafe_allow_html=True,
        )
    with refresh_col:
        if st.button("🔄 Yenile", use_container_width=True):
            manager.send_command("durum")
            time.sleep(0.3)
            st.rerun()

    card_cls = "status-open" if is_open else "status-closed"
    circle_cls = "circle-open" if is_open else "circle-closed"
    icon = "🔓" if is_open else "🔒"
    title_color = "#10B981" if is_open else "#F59E0B"
    title_text = "KAPAK AÇIK" if is_open else "KAPAK KAPALI"
    desc_text = (
        f"Mama kabı kapağı şu an açık ({state['aci']}°)."
        if is_open
        else f"Mama kabı kapağı şu an kapalı ({state['aci']}°)."
    )

    st.markdown(
        f"""
        <div class="status-card {card_cls}">
            <div class="muted-text" style="font-size:12px; font-weight:700; letter-spacing:1.2px; margin-bottom:14px;">
                MAMA KABI DURUMU
            </div>
            <div class="circle-badge {circle_cls}">{icon}</div>
            <div style="font-size:26px; font-weight:800; color:{title_color}; letter-spacing:0.6px;">
                {title_text}
            </div>
            <div class="muted-text" style="font-size:14px; margin-top:6px;">
                {desc_text}
            </div>
            <div class="metrics-bar">
                <span>⚙️ Servo: <b>{state['aci']}°</b> ({'Açık' if is_open else 'Kapalı'})</span>
                <span>🕒 Son İşlem: <b>{state['last_updated']}</b></span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


render_live_status()

st.markdown(
    '<div class="muted-text" style="font-size:12px; font-weight:700; letter-spacing:1px; margin-bottom:8px;">KONTROL TUŞLARI</div>',
    unsafe_allow_html=True,
)

col1, col2 = st.columns(2)

with col1:
    if st.button("🔓 KAPAĞI AÇ", use_container_width=True, type="primary"):
        manager.send_command("ac")
        time.sleep(0.35)
        st.rerun()

with col2:
    if st.button("🔒 KAPAĞI KAPAT", use_container_width=True):
        manager.send_command("kapat")
        time.sleep(0.35)
        st.rerun()

if st.button(
    "🍽️ 1 PORSİYON MAMA DÖK (1 Saniye)",
    use_container_width=True,
):
    manager.send_command("besle:1")
    time.sleep(0.35)
    st.rerun()
