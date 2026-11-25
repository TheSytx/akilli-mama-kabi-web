import hmac
import json
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

import paho.mqtt.client as mqtt
import streamlit as st

TR_TZ = timezone(timedelta(hours=3))

# Temiz Vector SVG İkonlar (Windows'un eski sistem emojileri yerine modern ikonlar)
SVG_PAW = """
<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#10B981" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
  <circle cx="11" cy="4" r="2"/><circle cx="18" cy="8" r="2"/><circle cx="20" cy="16" r="2"/>
  <path d="M9 10a5 5 0 0 1 5 5v3.5a3.5 3.5 0 0 1-6.84 1.045Q6.52 17.48 4.46 16.84A3.5 3.5 0 0 1 5.5 10Z"/>
</svg>
"""

SVG_LOCK_OPEN = """
<svg width="52" height="52" viewBox="0 0 24 24" fill="none" stroke="#10B981" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
  <rect width="18" height="11" x="3" y="11" rx="2" ry="2"/>
  <path d="M7 11V7a5 5 0 0 1 9.9-1"/>
</svg>
"""

SVG_LOCK_CLOSED = """
<svg width="52" height="52" viewBox="0 0 24 24" fill="none" stroke="#F59E0B" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
  <rect width="18" height="11" x="3" y="11" rx="2" ry="2"/>
  <path d="M7 11V7a5 5 0 0 1 10 0v4"/>
</svg>
"""

SVG_CHIP = """
<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
  <rect width="16" height="16" x="4" y="4" rx="2"/><rect width="6" height="6" x="9" y="9" rx="1"/>
  <path d="M15 2v2"/><path d="M15 20v2"/><path d="M2 15h2"/><path d="M2 9h2"/>
  <path d="M20 15h2"/><path d="M20 9h2"/><path d="M9 2v2"/><path d="M9 20v2"/>
</svg>
"""

SVG_CLOCK = """
<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
  <circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/>
</svg>
"""


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


# --- Sayfa Yapılandırması ---
st.set_page_config(
    page_title="Akıllı Mama Kabı",
    page_icon="🐾",
    layout="centered",
)

# --- Mobil Arayüzle Birebir Uyumlu Modern CSS (Light & Dark uyumlu) ---
st.markdown(
    """
    <style>
    .block-container {
        max-width: 560px !important;
        padding-top: 2.2rem !important;
        padding-bottom: 2.5rem !important;
    }

    /* Şifre kutusu: type="text" kalır, görsel olarak maskelenir (1Password vb. tetiklenmez) */
    input[aria-label="Erişim Kodu"] {
        -webkit-text-security: disc !important;
        text-security: disc !important;
    }

    /* Üst Başlık */
    .header-box {
        display: flex;
        align-items: center;
        gap: 12px;
    }
    .logo-badge {
        width: 46px;
        height: 46px;
        border-radius: 14px;
        background: rgba(16, 185, 129, 0.12);
        border: 1px solid rgba(16, 185, 129, 0.32);
        display: flex;
        align-items: center;
        justify-content: center;
        flex-shrink: 0;
    }
    .app-title {
        font-size: 22px;
        font-weight: 800;
        letter-spacing: -0.2px;
        margin: 0;
        color: var(--text-color);
    }

    /* Tek Parça Durum Kartı (Rozetler + Yenile Aynı Kutu İçinde Tam Ortalanmış) */
    .st-key-status_bar_container {
        background: var(--secondary-background-color) !important;
        border: 1px solid rgba(148, 163, 184, 0.2) !important;
        border-radius: 16px !important;
        padding: 10px 14px !important;
        margin-top: 4px;
    }
    .st-key-status_bar_container > div,
    .st-key-status_bar_container [data-testid="stVerticalBlock"],
    .st-key-status_bar_container [data-testid="stColumn"] > div {
        gap: 0 !important;
        padding-top: 0 !important;
        padding-bottom: 0 !important;
        justify-content: center !important;
    }
    .st-key-status_bar_container [data-testid="stHorizontalBlock"] {
        align-items: center !important;
    }
    .st-key-status_bar_container [data-testid="stElementContainer"],
    .st-key-status_bar_container [data-testid="stMarkdown"],
    .st-key-status_bar_container [data-testid="stMarkdownContainer"],
    .st-key-status_bar_container [data-testid="stMarkdownContainer"] p {
        margin: 0 !important;
        padding: 0 !important;
        display: flex !important;
        align-items: center !important;
    }
    .pills-wrap {
        display: flex;
        align-items: center;
        flex-wrap: wrap;
        gap: 8px;
        min-height: 36px;
    }
    .status-pill {
        display: inline-flex;
        align-items: center;
        gap: 7px;
        padding: 6px 12px;
        border-radius: 999px;
        font-size: 12.5px;
        font-weight: 700;
        line-height: 1.1;
        white-space: nowrap;
    }
    .pill-green {
        background: rgba(16, 185, 129, 0.13);
        border: 1px solid rgba(16, 185, 129, 0.35);
        color: #10B981;
    }
    .pill-amber {
        background: rgba(245, 158, 11, 0.13);
        border: 1px solid rgba(245, 158, 11, 0.35);
        color: #F59E0B;
    }
    .pill-red {
        background: rgba(239, 68, 68, 0.13);
        border: 1px solid rgba(239, 68, 68, 0.35);
        color: #EF4444;
    }
    .pill-dot {
        width: 7px;
        height: 7px;
        border-radius: 999px;
        display: inline-block;
    }

    /* Kompakt Çıkış ve Yenile Butonları */
    .st-key-btn_logout button,
    .st-key-btn_refresh button {
        border-radius: 12px !important;
        padding: 0.42rem 0.75rem !important;
        min-height: 36px !important;
        font-size: 13px !important;
        font-weight: 600 !important;
        border: 1px solid rgba(148, 163, 184, 0.28) !important;
        background: rgba(148, 163, 184, 0.08) !important;
        color: var(--text-color) !important;
        transition: all 0.15s ease !important;
    }
    .st-key-btn_logout button:hover,
    .st-key-btn_refresh button:hover {
        border-color: rgba(148, 163, 184, 0.55) !important;
        background: rgba(148, 163, 184, 0.16) !important;
    }

    /* Büyük Mama Kabı Durum Kartı */
    .status-card {
        background: var(--secondary-background-color);
        border-radius: 24px;
        padding: 26px 20px 20px 20px;
        text-align: center;
        margin: 8px 0 14px 0;
        transition: border-color 0.2s ease;
    }
    .status-open {
        border: 1.5px solid rgba(16, 185, 129, 0.5);
    }
    .status-closed {
        border: 1.5px solid rgba(245, 158, 11, 0.42);
    }
    .circle-badge {
        width: 118px;
        height: 118px;
        border-radius: 59px;
        margin: 0 auto 16px auto;
        display: flex;
        align-items: center;
        justify-content: center;
    }
    .circle-open {
        background: rgba(16, 185, 129, 0.12);
        border: 3px solid #10B981;
    }
    .circle-closed {
        background: rgba(245, 158, 11, 0.12);
        border: 3px solid #F59E0B;
    }
    .metrics-bar {
        display: flex;
        justify-content: space-around;
        align-items: center;
        background: var(--background-color);
        border: 1px solid rgba(148, 163, 184, 0.16);
        border-radius: 12px;
        padding: 10px 14px;
        margin-top: 18px;
        font-size: 13px;
        color: var(--text-color);
    }
    .metric-item {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        opacity: 0.9;
    }
    .metric-divider {
        width: 1px;
        height: 16px;
        background: rgba(148, 163, 184, 0.25);
    }
    .muted-label {
        font-size: 11.5px;
        font-weight: 700;
        letter-spacing: 1.2px;
        color: var(--text-color);
        opacity: 0.55;
        text-transform: uppercase;
    }
    .muted-desc {
        font-size: 13.5px;
        color: var(--text-color);
        opacity: 0.72;
        margin-top: 6px;
    }

    /* Kontrol Tuşları — Mobil Uygulamadaki Büyük Kart Buton Tasarımı */
    .st-key-btn_open_active button,
    .st-key-btn_open_idle button {
        min-height: 76px !important;
        border-radius: 18px !important;
        font-size: 15px !important;
        font-weight: 800 !important;
        letter-spacing: 0.4px !important;
        color: #FFFFFF !important;
        transition: transform 0.12s ease, filter 0.15s ease !important;
    }
    .st-key-btn_open_active button {
        background: #059669 !important;
        border: 2px solid #34D399 !important;
    }
    .st-key-btn_open_idle button {
        background: #064E3B !important;
        border: 2px solid rgba(16, 185, 129, 0.35) !important;
    }
    .st-key-btn_open_active button:hover,
    .st-key-btn_open_idle button:hover {
        filter: brightness(1.1);
    }

    .st-key-btn_close_active button,
    .st-key-btn_close_idle button {
        min-height: 76px !important;
        border-radius: 18px !important;
        font-size: 15px !important;
        font-weight: 800 !important;
        letter-spacing: 0.4px !important;
        color: #FFFFFF !important;
        transition: transform 0.12s ease, filter 0.15s ease !important;
    }
    .st-key-btn_close_active button {
        background: #D97706 !important;
        border: 2px solid #FBBF24 !important;
    }
    .st-key-btn_close_idle button {
        background: #451A03 !important;
        border: 2px solid rgba(245, 158, 11, 0.35) !important;
    }
    .st-key-btn_close_active button:hover,
    .st-key-btn_close_idle button:hover {
        filter: brightness(1.12);
    }

    /* 1 Porsiyon Mama Dök Butonu */
    .st-key-btn_feed button {
        min-height: 54px !important;
        border-radius: 16px !important;
        font-size: 14.5px !important;
        font-weight: 700 !important;
        background: #1E293B !important;
        border: 1.5px solid #334155 !important;
        color: #F8FAFC !important;
        margin-top: 2px;
        transition: all 0.15s ease !important;
    }
    .st-key-btn_feed button:hover {
        background: #273549 !important;
        border-color: #475569 !important;
    }

    /* Giriş & Yüklenme Kartları */
    .login-box {
        background: var(--secondary-background-color);
        border: 1px solid rgba(148, 163, 184, 0.2);
        border-radius: 22px;
        padding: 26px 22px 18px 22px;
        text-align: center;
        margin-top: 36px;
        margin-bottom: 14px;
    }
    .st-key-btn_login button {
        min-height: 46px !important;
        border-radius: 14px !important;
        font-size: 15px !important;
        font-weight: 700 !important;
        background: #059669 !important;
        border: 1px solid #34D399 !important;
        color: #FFFFFF !important;
    }
    .loading-card {
        background: var(--secondary-background-color);
        border: 1px solid rgba(16, 185, 129, 0.3);
        border-radius: 22px;
        padding: 44px 24px;
        text-align: center;
        margin-top: 48px;
    }
    .spinner-ring {
        width: 42px;
        height: 42px;
        border: 3.5px solid rgba(16, 185, 129, 0.2);
        border-top-color: #10B981;
        border-radius: 50%;
        margin: 18px auto 14px auto;
        animation: kapspin 0.8s linear infinite;
    }
    @keyframes kapspin {
        to { transform: rotate(360deg); }
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


# --- 1. Basit Giriş Ekranı ---
if not st.session_state.authenticated:
    st.markdown(
        f"""
        <div class="login-box">
            <div class="logo-badge" style="margin:0 auto 12px auto;">{SVG_PAW}</div>
            <div class="app-title">Giriş</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.text_input(
        "Erişim Kodu",
        key="access_code_input",
        type="default",
        autocomplete="off",
        placeholder="Şifreyi giriniz...",
        label_visibility="collapsed",
        on_change=try_login,
    )

    if st.button(
        "Giriş Yap",
        key="btn_login",
        use_container_width=True,
        icon=":material/login:",
    ):
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
        f"""
        <div class="loading-card">
            <div class="logo-badge" style="margin:0 auto;">{SVG_PAW}</div>
            <div class="spinner-ring"></div>
            <div style="font-size:17px; font-weight:700; color:var(--text-color);">
                Kontrol Paneli Hazırlanıyor
            </div>
            <div class="muted-desc">
                İletişim sunucusuna bağlanılıyor...
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    mgr = get_mqtt_manager(MQTT_BROKER, MQTT_PORT, TOPIC_PREFIX)
    mgr.send_command("durum")
    time.sleep(0.55)
    st.session_state.is_loading_panel = False
    placeholder.empty()
    st.rerun()


# --- 3. Ana Kontrol Paneli ---
manager = get_mqtt_manager(MQTT_BROKER, MQTT_PORT, TOPIC_PREFIX)

header_col, logout_col = st.columns([3.8, 1.2], vertical_alignment="center")
with header_col:
    st.markdown(
        f"""
        <div class="header-box">
            <div class="logo-badge">{SVG_PAW}</div>
            <h2 class="app-title">Akıllı Mama Kabı</h2>
        </div>
        """,
        unsafe_allow_html=True,
    )
with logout_col:
    if st.button(
        "Çıkış",
        key="btn_logout",
        use_container_width=True,
        icon=":material/logout:",
    ):
        st.session_state.authenticated = False
        st.session_state.is_loading_panel = False
        st.rerun()


@st.fragment(run_every="2s")
def render_dashboard():
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
            'Bağlanılıyor...'
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

    # Tek Parça Durum Çubuğu (Rozetler Solda, Yenile Sağda Aynı Kutu İçinde)
    with st.container(border=True, key="status_bar_container"):
        pills_col, refresh_col = st.columns([3.8, 1.2], vertical_alignment="center")
        with pills_col:
            st.markdown(
                f'<div class="pills-wrap">{server_pill}{device_pill}</div>',
                unsafe_allow_html=True,
            )
        with refresh_col:
            if st.button(
                "Yenile",
                key="btn_refresh",
                use_container_width=True,
                icon=":material/refresh:",
            ):
                manager.send_command("durum")
                time.sleep(0.3)
                st.rerun()

    # Büyük Durum Kartı
    card_cls = "status-open" if is_open else "status-closed"
    circle_cls = "circle-open" if is_open else "circle-closed"
    lock_svg = SVG_LOCK_OPEN if is_open else SVG_LOCK_CLOSED
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
            <div class="muted-label" style="margin-bottom:16px;">MAMA KABI DURUMU</div>
            <div class="circle-badge {circle_cls}">{lock_svg}</div>
            <div style="font-size:25px; font-weight:800; color:{title_color}; letter-spacing:0.6px;">
                {title_text}
            </div>
            <div class="muted-desc">{desc_text}</div>
            <div class="metrics-bar">
                <span class="metric-item">{SVG_CHIP} Servo: <b>{state['aci']}°</b> ({'Açık' if is_open else 'Kapalı'})</span>
                <span class="metric-divider"></span>
                <span class="metric-item">{SVG_CLOCK} Son İşlem: <b>{state['last_updated']}</b></span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Kontrol Tuşları (Aktif duruma göre parlak yeşil / kehribar)
    st.markdown(
        '<div class="muted-label" style="margin:6px 0 10px 4px;">KONTROL TUŞLARI</div>',
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns(2)
    with col1:
        open_key = "btn_open_active" if is_open else "btn_open_idle"
        if st.button(
            "KAPAĞI AÇ",
            key=open_key,
            use_container_width=True,
            icon=":material/lock_open:",
        ):
            manager.send_command("ac")
            time.sleep(0.35)
            st.rerun()

    with col2:
        close_key = "btn_close_active" if not is_open else "btn_close_idle"
        if st.button(
            "KAPAĞI KAPAT",
            key=close_key,
            use_container_width=True,
            icon=":material/lock:",
        ):
            manager.send_command("kapat")
            time.sleep(0.35)
            st.rerun()

    if st.button(
        "1 PORSİYON MAMA DÖK (1 Saniye)",
        key="btn_feed",
        use_container_width=True,
        icon=":material/restaurant:",
    ):
        manager.send_command("besle:1")
        time.sleep(0.35)
        st.rerun()


render_dashboard()
