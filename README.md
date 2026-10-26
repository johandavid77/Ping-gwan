# 🐧 Ping-gwan

> Un monitor local ultrarrápido, ligero y autónomo de disponibilidad web (Uptime/Ping) escrito en **Rust**. Diseñado para ejecutarse en cualquier entorno local con cero dependencias externas pesadas.

---

## 🚀 Características Principales

- **Alto Rendimiento en Rust:** Motor asíncrono impulsado por `Tokio` y peticiones HTTP concurrentes con `Reqwest`.
- **Almacenamiento Local Robusto:** Base de datos `SQLite` integrada para almacenar históricos de latencia y registros de estado.
- **Dashboard Web en Tiempo Real:** Interfaz moderna impulsada por `Axum`, estilizada con `Tailwind CSS` y gráficos dinámicos con `Chart.js`.
- **Configuración Declarativa (`config.toml`):** Administra intervalos, puertos y objetivos de manera limpia mediante archivos TOML.
- **Notificaciones Remotas:** Sistema de alertas integrado para notificar caídas de servicios directamente a través de **Telegram** (y preparado para webhooks de WhatsApp).
- **Gestión Interactiva:** Añade, pausa, consulta gráficos de velocidad en el tiempo o elimina objetivos directamente desde el panel web.

---

## 📂 Estructura del Proyecto

```text
ping-gwan/
├── Cargo.toml          # Dependencias y metadatos del proyecto
├── config.toml         # Configuración general, objetivos y credenciales de alertas
├── pinggwan.db         # Base de datos SQLite local (generada automáticamente)
├── index.html          # Interfaz gráfica del Dashboard Web
└── src/
    └── main.rs         # Lógica del daemon, motor de monitoreo y servidor API REST