from dotenv import load_dotenv
import os

load_dotenv()

REQUIRED_KEYS = [
    "ANTHROPIC_API_KEY",
    "PIPEDRIVE_API_TOKEN",
    "JOBBER_ACCESS_TOKEN",
    "QBO_ACCESS_TOKEN",
    "QBO_COMPANY_ID",
    "ASANA_ACCESS_TOKEN",
    "ASANA_WORKSPACE_GID",
    "HUBSPOT_ACCESS_TOKEN",
    "MAILCHIMP_API_KEY",
    "MAILCHIMP_SERVER_PREFIX",
    "SLACK_BOT_TOKEN",
    "GOOGLE_CREDENTIALS_FILE",
]


def get_credential(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise EnvironmentError(
            f"Missing required credential: '{key}'. "
            f"Please set it in your .env file (see .env.example)."
        )
    return value


def verify_all() -> bool:
    all_present = True
    for key in REQUIRED_KEYS:
        value = os.getenv(key)
        if value:
            print(f"  OK      {key}")
        else:
            print(f"  MISSING {key}")
            all_present = False
    return all_present


if __name__ == "__main__":
    print("Verifying credentials...\n")
    result = verify_all()
    print()
    if result:
        print("All credentials present.")
    else:
        print("One or more credentials are missing.")
