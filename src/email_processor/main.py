import logging
from pathlib import Path

from config.settings import Settings
from email_processor.service import EmailProcessorService


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    settings = Settings()

    service = EmailProcessorService(settings)

    sql_path = Path(__file__).resolve().parents[2] / "sql" / "init.sql"
    service.pg_store.init_schema(str(sql_path))

    service.run()


if __name__ == "__main__":
    main()
