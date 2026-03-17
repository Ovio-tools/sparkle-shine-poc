from dotenv import load_dotenv
import os

load_dotenv()

REQUIRED_KEYS = [
    "ANTHROPIC_API_KEY",
    "PIPEDRIVE_API_TOKEN",
    "JOBBER_CLIENT_ID",
    "JOBBER_CLIENT_SECRET",
    "QUICKBOOKS_CLIENT_ID",
    "QUICKBOOKS_CLIENT_SECRET",
    "QUICKBOOKS_SANDBOX_COMPANY_ID",
    "ASANA_PAT",
    "HUBSPOT_TOKEN",
    "MAILCHIMP_API_KEY",
    "MAILCHIMP_DATA_CENTER",
    "SLACK_BOT_TOKEN",
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
