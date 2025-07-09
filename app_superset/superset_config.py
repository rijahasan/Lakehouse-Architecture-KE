from superset.utils.core import DASHBOARD_POSITION_DATA_VERSION_KEY

THEME_OVERRIDES = {
    "colors": {
        "primary": "#4B0082",
        "secondary": "#8A2BE2",
        "success": "#228B22",
        "info": "#1E90FF",
        "warning": "#FFD700",
        "danger": "#DC143C",
        "light": "#F8F9FA",
        "dark": "#343A40",
        # You can also define chart-specific color palettes
        "grayscale": [
            "#212529", "#343a40", "#495057", "#6c757d", "#adb5bd", "#ced4da", "#dee2e6", "#e9ecef", "#f8f9fa", "#ffffff"
        ],
        "categorical": [
            "#4B0082", "#8A2BE2", "#228B22", "#1E90FF", "#FFD700", "#DC143C", "#FF69B4", "#00CED1", "#7FFF00", "#FF4500"
        ]
    }
}
SQLLAB_TIMEOUT=120

FEATURE_FLAGS = {
    "ALERT_REPORTS": True,
    "SCHEDULED_QUERIES": True,
}

EMAIL_NOTIFICATIONS = True
SMTP_HOST = "smtp.gmail.com"  # Or your SMTP server
SMTP_PORT = 587
SMTP_USER = "your_email@gmail.com"
SMTP_PASSWORD = "your_app_password"
SMTP_MAIL_FROM = "your_email@gmail.com"
SMTP_STARTTLS = True
SMTP_SSL = False
