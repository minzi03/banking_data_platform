# =============================================================================
# Apache Superset — Configuration for Banking Data Platform
# =============================================================================

import os

# ── Core ────────────────────────────────────────────────────────────────────
# DATA_DIR is required by Superset 3.1.x initialization
# Set via SUPERSET_DATA_DIR env var or default to superset_home
DATA_DIR = os.environ.get("SUPERSET_DATA_DIR", "/app/superset_home")
os.makedirs(DATA_DIR, exist_ok=True)
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "banking_platform_secret_key_change_me")
SQLALCHEMY_DATABASE_URI = (
    f"postgresql://{os.environ.get('DATABASE_USER', 'banking_admin')}"
    f":{os.environ.get('DATABASE_PASSWORD', 'BankingAdmin123')}"
    f"@{os.environ.get('DATABASE_HOST', 'postgres')}"
    f":{os.environ.get('DATABASE_PORT', '5432')}"
    f"/{os.environ.get('DATABASE_DB', 'superset')}"
)

# ── Disable CSRF for API access ─────────────────────────────────────────────
WTF_CSRF_ENABLED = False

# ── Feature Flags ───────────────────────────────────────────────────────────
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "ENABLE_EXPLORE_DRAG_AND_DROP": True,
    "ENABLE_DND_WITH_CLICK_UX": True,
    "EMBEDDED_SUPERSET": True,
    "ENABLE_JAVASCRIPT_CONTROLS": True,
}

# ── Allow all origins for development ───────────────────────────────────────
CORS_OPTIONS = {
    "supports_credentials": True,
    "allow_headers": ["*"],
    "resources": ["*"],
    "origins": ["*"],
}
ENABLE_CORS = True

# ── Database Configs (Trino as primary data source) ────────────────────────
# Trino connection will be configured via UI or init script
# PostgreSQL as metadata store (this config)

# ── Row Level Security (for banking roles) ──────────────────────────────────
ROW_LEVEL_LIMITS = [
    # Future: role-based row filtering for banking data
    # {"role": "retail_analyst", "clause": "customer_segment = 'RETAIL'"},
]

# ── Caching ─────────────────────────────────────────────────────────────────
CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
}

DATA_CACHE_CONFIG = {
    **CACHE_CONFIG,
    "CACHE_DEFAULT_TIMEOUT": 600,
    "CACHE_KEY_PREFIX": "superset_data_",
}

# ── Theming ─────────────────────────────────────────────────────────────────
THEME = {
    "primary": "#1890ff",
    "secondary": "#1da57a",
    "success": "#52c41a",
    "warning": "#faad14",
    "error": "#f5222d",
}

# ── SQL Lab ─────────────────────────────────────────────────────────────────
SQLLAB_TIMEOUT = 300
SUPERSET_WEBSERVER_TIMEOUT = 300

# ── Public role (read-only for dashboards) ──────────────────────────────────
PUBLIC_ROLE_LIKE = "Gamma"

# ── App name ────────────────────────────────────────────────────────────────
APP_NAME = "Banking Data Platform"

# ── Languages ───────────────────────────────────────────────────────────────
LANGUAGES = {
    "en": {"flag": "us", "name": "English"},
    "vi": {"flag": "vn", "name": "Vietnamese"},
}
