use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{FromRow, SqlitePool};
use std::fs;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;
use tower_http::cors::CorsLayer;

// --- ESTRUCTURAS DE CONFIGURACIÓN ---
#[derive(Debug, Serialize, Deserialize, Clone)]
struct Config {
    general: GeneralConfig,
    notifications: NotificationsConfig,
    targets: Vec<TargetConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct GeneralConfig {
    check_interval_secs: u64,
    database_path: String,
    web_port: u16,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct NotificationsConfig {
    telegram: TelegramConfig,
    whatsapp: WhatsappConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct TelegramConfig {
    enabled: bool,
    bot_token: String,
    chat_id: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct WhatsappConfig {
    enabled: bool,
    webhook_url: String,
    api_token: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct TargetConfig {
    name: String,
    url: String,
}

// --- MODELOS DE BD ---
#[derive(Debug, FromRow, Serialize, Deserialize, Clone)]
struct TargetRow {
    id: i64,
    name: String,
    url: String,
    active: bool,
}

#[derive(Debug, FromRow, Serialize, Deserialize, Clone)]
struct UptimeLogRow {
    id: i64,
    target_id: i64,
    status_code: Option<i64>,
    latency_ms: Option<i64>,
    is_up: bool,
    checked_at: String,
}

#[derive(Debug, Serialize)]
struct TargetStatus {
    target: TargetRow,
    last_status_code: Option<i64>,
    last_latency_ms: Option<i64>,
    is_up: bool,
    last_check: Option<String>,
    first_check: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CreateTargetPayload {
    name: String,
    url: String,
}

#[derive(Clone)]
struct AppState {
    pool: SqlitePool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    println!("🚀 Iniciando Ping-gwan (Motor de Uptime + Dashboard Web + Tiempos de Monitoreo)...");

    // 1. Cargar o crear config.toml
    let config_path = "config.toml";
    if !std::path::Path::new(config_path).exists() {
        let default_config = r#"[general]
check_interval_secs = 30
database_path = "pinggwan.db"
web_port = 9090

[notifications.telegram]
enabled = false
bot_token = ""
chat_id = ""

[notifications.whatsapp]
enabled = false
webhook_url = ""
api_token = ""

[[targets]]
name = "Google Cloud"
url = "https://www.google.com"

[[targets]]
name = "Sitio Web Prueba HTTP"
url = "https://httpbin.org/status/200"
"#;
        fs::write(config_path, default_config)?;
        println!("📝 Archivo 'config.toml' creado con valores por defecto.");
    }

    let config_content = fs::read_to_string(config_path)?;
    let config: Config = toml::from_str(&config_content)?;

    // 2. Conexión a SQLite
    let database_url = format!("sqlite:{}?mode=rwc", config.general.database_path);
    let pool = SqlitePoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;

    // 3. Crear tablas
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS targets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            url TEXT NOT NULL,
            active BOOLEAN NOT NULL DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS uptime_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            target_id INTEGER NOT NULL,
            status_code INTEGER,
            latency_ms INTEGER,
            is_up BOOLEAN NOT NULL,
            checked_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (target_id) REFERENCES targets(id)
        );
        "#,
    )
    .execute(&pool)
    .await?;

    // 4. Sincronizar targets iniciales
    for t in &config.targets {
        sqlx::query(
            r#"
            INSERT INTO targets (name, url, active) VALUES (?, ?, 1)
            ON CONFLICT(name) DO UPDATE SET url = excluded.url;
            "#,
        )
        .bind(&t.name)
        .bind(&t.url)
        .execute(&pool)
        .await?;
    }

    // --- TAREA EN SEGUNDO PLANO: MOTOR DE MONITOREO ---
    let pool_worker = pool.clone();
    tokio::spawn(async move {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap();

        loop {
            let current_config: Option<Config> = fs::read_to_string("config.toml")
                .ok()
                .and_then(|content| toml::from_str(&content).ok());

            let check_interval = current_config
                .as_ref()
                .map(|c| c.general.check_interval_secs)
                .unwrap_or(30);

            if let Ok(targets) = sqlx::query_as::<_, TargetRow>(
                "SELECT id, name, url, active FROM targets WHERE active = 1",
            )
            .fetch_all(&pool_worker)
            .await
            {
                for target in targets {
                    let client_clone = client.clone();
                    let pool_clone = pool_worker.clone();
                    let cfg_clone = current_config.clone();

                    tokio::spawn(async move {
                        let start = std::time::Instant::now();
                        let res = client_clone.get(&target.url).send().await;
                        let elapsed = start.elapsed().as_millis() as i64;

                        let (is_up, status_code) = match res {
                            Ok(response) => {
                                let status = response.status().as_u16();
                                (response.status().is_success(), Some(status as i64))
                            }
                            Err(_) => (false, None),
                        };

                        if !is_up {
                            if let Some(cfg) = &cfg_clone {
                                send_telegram_alert(&client_clone, &cfg.notifications.telegram, &target.name, &target.url).await;
                            }
                        }

                        let _ = sqlx::query(
                            r#"
                            INSERT INTO uptime_logs (target_id, status_code, latency_ms, is_up)
                            VALUES (?, ?, ?, ?)
                            "#,
                        )
                        .bind(target.id)
                        .bind(status_code)
                        .bind(elapsed)
                        .bind(is_up)
                        .execute(&pool_clone)
                        .await;
                    });
                }
            }

            sleep(Duration::from_secs(check_interval)).await;
        }
    });

    // --- RUTAS API AXUM ---
    let state = AppState { pool };

    let app = Router::new()
        .route("/", get(serve_dashboard))
        .route("/api/targets", get(get_targets).post(create_target))
        .route("/api/targets/:id", delete(delete_target))
        .route("/api/targets/:id/toggle", post(toggle_target))
        .route("/api/targets/:id/logs", get(get_target_logs))
        .route("/api/logs", get(get_recent_logs))
        .route("/api/config", get(get_config).post(update_notifications_config))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let port = config.general.web_port;
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    println!("🌐 Dashboard Web disponible en: http://localhost:{}", port);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn send_telegram_alert(
    client: &reqwest::Client,
    tg_cfg: &TelegramConfig,
    target_name: &str,
    target_url: &str,
) {
    if !tg_cfg.enabled || tg_cfg.bot_token.is_empty() || tg_cfg.chat_id.is_empty() {
        return;
    }

    let message = format!(
        "🚨 *[PING-GWAN ALERT]*\n\nEl servicio *{}* está caído.\n🔗 URL: {}\n⏰ Hora: {}",
        target_name,
        target_url,
        chrono::Local::now().format("%Y-%m-%d %H:%M:%S")
    );

    let api_url = format!(
        "https://api.telegram.org/bot{}/sendMessage",
        tg_cfg.bot_token
    );

    let body = serde_json::json!({
        "chat_id": tg_cfg.chat_id,
        "text": message,
        "parse_mode": "Markdown"
    });

    let _ = client.post(&api_url).json(&body).send().await;
}

// --- HANDLERS API ---

async fn serve_dashboard() -> impl IntoResponse {
    let html = fs::read_to_string("index.html").unwrap_or_else(|_| {
        "<h1>Ping-gwan Dashboard</h1><p>Asegúrate de colocar 'index.html' en la raíz del proyecto.</p>".to_string()
    });
    Html(html)
}

async fn get_targets(State(state): State<AppState>) -> Result<Json<Vec<TargetStatus>>, StatusCode> {
    let targets = sqlx::query_as::<_, TargetRow>("SELECT id, name, url, active FROM targets")
        .fetch_all(&state.pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut result = Vec::new();

    for target in targets {
        // Último log
        let last_log = sqlx::query_as::<_, UptimeLogRow>(
            r#"
            SELECT id, target_id, status_code, latency_ms, is_up,
                   datetime(checked_at, 'localtime') as checked_at
            FROM uptime_logs
            WHERE target_id = ?
            ORDER BY id DESC LIMIT 1
            "#,
        )
        .bind(target.id)
        .fetch_optional(&state.pool)
        .await
        .unwrap_or(None);

        // Primer log (inicio de monitoreo)
        let first_log = sqlx::query_as::<_, (Option<String>,)>(
            r#"
            SELECT datetime(MIN(checked_at), 'localtime')
            FROM uptime_logs
            WHERE target_id = ?
            "#,
        )
        .bind(target.id)
        .fetch_optional(&state.pool)
        .await
        .unwrap_or(None);

        let (last_status_code, last_latency_ms, is_up, last_check) = match last_log {
            Some(log) => (log.status_code, log.latency_ms, log.is_up, Some(log.checked_at)),
            None => (None, None, true, None),
        };

        let first_check = first_log.and_then(|r| r.0);

        result.push(TargetStatus {
            target,
            last_status_code,
            last_latency_ms,
            is_up,
            last_check,
            first_check,
        });
    }

    Ok(Json(result))
}

async fn create_target(
    State(state): State<AppState>,
    Json(payload): Json<CreateTargetPayload>,
) -> Result<StatusCode, StatusCode> {
    sqlx::query("INSERT INTO targets (name, url, active) VALUES (?, ?, 1)")
        .bind(&payload.name)
        .bind(&payload.url)
        .execute(&state.pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(StatusCode::CREATED)
}

async fn delete_target(
    Path(id): Path<i64>,
    State(state): State<AppState>,
) -> Result<StatusCode, StatusCode> {
    let _ = sqlx::query("DELETE FROM uptime_logs WHERE target_id = ?")
        .bind(id)
        .execute(&state.pool)
        .await;

    let res = sqlx::query("DELETE FROM targets WHERE id = ?")
        .bind(id)
        .execute(&state.pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if res.rows_affected() == 0 {
        return Err(StatusCode::NOT_FOUND);
    }

    Ok(StatusCode::OK)
}

async fn toggle_target(
    Path(id): Path<i64>,
    State(state): State<AppState>,
) -> Result<StatusCode, StatusCode> {
    sqlx::query("UPDATE targets SET active = NOT active WHERE id = ?")
        .bind(id)
        .execute(&state.pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(StatusCode::OK)
}

async fn get_target_logs(
    Path(id): Path<i64>,
    State(state): State<AppState>,
) -> Result<Json<Vec<UptimeLogRow>>, StatusCode> {
    let logs = sqlx::query_as::<_, UptimeLogRow>(
        r#"
        SELECT id, target_id, status_code, latency_ms, is_up,
               datetime(checked_at, 'localtime') as checked_at
        FROM uptime_logs
        WHERE target_id = ?
        ORDER BY id ASC LIMIT 50
        "#,
    )
    .bind(id)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(logs))
}

async fn get_recent_logs(
    State(state): State<AppState>,
) -> Result<Json<Vec<UptimeLogRow>>, StatusCode> {
    let logs = sqlx::query_as::<_, UptimeLogRow>(
        r#"
        SELECT id, target_id, status_code, latency_ms, is_up,
               datetime(checked_at, 'localtime') as checked_at
        FROM uptime_logs
        ORDER BY id DESC LIMIT 50
        "#,
    )
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(logs))
}

async fn get_config() -> Result<Json<NotificationsConfig>, StatusCode> {
    let config_content = fs::read_to_string("config.toml").map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let config: Config = toml::from_str(&config_content).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(config.notifications))
}

async fn update_notifications_config(
    Json(payload): Json<NotificationsConfig>,
) -> Result<StatusCode, StatusCode> {
    let config_content = fs::read_to_string("config.toml").map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut config: Config = toml::from_str(&config_content).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    config.notifications = payload;

    let new_toml = toml::to_string_pretty(&config).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    fs::write("config.toml", new_toml).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(StatusCode::OK)
}