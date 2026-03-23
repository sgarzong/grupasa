from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
import os


BASE_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = BASE_DIR / ".env"
load_dotenv(ENV_PATH)


@dataclass(frozen=True)
class Settings:
    base_dir: Path
    source_xlsx_url: str
    source_local_path: Path | None
    cas_alert_days: int
    timezone_name: str
    raw_dir: Path
    curated_dir: Path
    history_dir: Path
    quality_dir: Path
    logs_dir: Path
    source_latest_path: Path
    source_archive_path: Path
    contenedores_actual_path: Path
    status_historico_path: Path
    registro_congelado_path: Path
    plan_galagans_congelado_path: Path
    errores_validacion_path: Path
    log_file_path: Path

    @property
    def snapshot_date(self) -> str:
        timezone = ZoneInfo(self.timezone_name)
        return datetime.now(timezone).date().isoformat()


def get_settings() -> Settings:
    raw_dir = BASE_DIR / "data" / "raw"
    curated_dir = BASE_DIR / "data" / "curated"
    history_dir = BASE_DIR / "data" / "history"
    quality_dir = BASE_DIR / "data" / "quality"
    logs_dir = BASE_DIR / "logs"
    timezone_name = os.getenv("TIMEZONE", "America/Guayaquil")
    snapshot_date = datetime.now(ZoneInfo(timezone_name)).date()

    source_local = os.getenv("SOURCE_LOCAL_PATH", "").strip()
    source_local_path = Path(source_local).expanduser().resolve() if source_local else None

    return Settings(
        base_dir=BASE_DIR,
        source_xlsx_url=os.getenv(
            "SOURCE_XLSX_URL",
            "https://docs.google.com/spreadsheets/d/1v0E_F2QbjWOGOR93vWT_hZEeNnK06CVX/export?format=xlsx",
        ),
        source_local_path=source_local_path,
        cas_alert_days=int(os.getenv("CAS_ALERT_DAYS", "3")),
        timezone_name=timezone_name,
        raw_dir=raw_dir,
        curated_dir=curated_dir,
        history_dir=history_dir,
        quality_dir=quality_dir,
        logs_dir=logs_dir,
        source_latest_path=raw_dir / "source_latest.xlsx",
        source_archive_path=raw_dir / f"source_{snapshot_date.strftime('%Y%m%d')}.xlsx",
        contenedores_actual_path=curated_dir / "contenedores_actual.csv",
        status_historico_path=history_dir / "status_historico.csv",
        registro_congelado_path=history_dir / "registro_congelado.csv",
        plan_galagans_congelado_path=history_dir / "plan_galagans_congelado.csv",
        errores_validacion_path=quality_dir / "errores_validacion.csv",
        log_file_path=logs_dir / "pipeline.log",
    )


def ensure_directories(settings: Settings) -> None:
    for path in (
        settings.raw_dir,
        settings.curated_dir,
        settings.history_dir,
        settings.quality_dir,
        settings.logs_dir,
    ):
        path.mkdir(parents=True, exist_ok=True)
