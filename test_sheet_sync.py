from src.sheet_sync import SheetSync

# Hardcode your Apps Script URLs here
POST_WEBHOOK = "https://script.google.com/macros/s/AKfycbwpB3usOwJu5dxcbYMqQgf8dEkCeAQIZ1UWKfWVhRqh124tBhBjgK6lUtoBc1TA3H19/exec"
GET_WEBHOOK = "https://script.google.com/macros/s/AKfycbwpB3usOwJu5dxcbYMqQgf8dEkCeAQIZ1UWKfWVhRqh124tBhBjgK6lUtoBc1TA3H19/exec"  # or "" / None if you don't use GET

def main():
    sync = SheetSync(webhook_post=POST_WEBHOOK, webhook_get=GET_WEBHOOK)
    rows_sent = sync.sync()
    print(f"Rows sent: {rows_sent}")

if __name__ == "__main__":
    main()

