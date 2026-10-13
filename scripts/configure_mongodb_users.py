"""Create local MongoDB users and an ignored connection environment file."""

import secrets
from pathlib import Path
from urllib.parse import quote_plus

from pymongo import MongoClient


def main() -> None:
    environment_path = Path(__file__).resolve().parents[1] / ".env"
    if environment_path.exists():
        raise RuntimeError(
            ".env already exists. Remove it only if MongoDB credentials must be reset."
        )

    admin_password = secrets.token_urlsafe(32)
    application_password = secrets.token_urlsafe(32)
    client = MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    client.admin.command(
        "createUser",
        "flight_admin",
        pwd=admin_password,
        roles=[{"role": "root", "db": "admin"}],
    )
    client["flight_delay"].command(
        "createUser",
        "flight_app",
        pwd=application_password,
        roles=[{"role": "readWrite", "db": "flight_delay"}],
    )
    client.close()

    admin_uri = (
        "mongodb://flight_admin:"
        f"{quote_plus(admin_password)}@127.0.0.1:27017/admin?authSource=admin"
    )
    application_uri = (
        "mongodb://flight_app:"
        f"{quote_plus(application_password)}@127.0.0.1:27017/"
        "flight_delay?authSource=flight_delay"
    )
    environment_path.write_text(
        f"MONGODB_URI={application_uri}\nMONGODB_ADMIN_URI={admin_uri}\n",
        encoding="utf-8",
    )
    environment_path.chmod(0o600)
    print(f"MongoDB users created. Credentials saved to {environment_path}")


if __name__ == "__main__":
    main()
