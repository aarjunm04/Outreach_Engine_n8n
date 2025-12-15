from src.sheet_sync import SheetSync

POST_URL = "https://script.google.com/macros/s/AKfycbwpB3usOwJu5dxcbYMqQgf8dEkCeAQIZ1UWKfWVhRqh124tBhBjgK6lUtoBc1TA3H19/exec"
GET_URL = "https://script.google.com/macros/s/AKfycbwpB3usOwJu5dxcbYMqQgf8dEkCeAQIZ1UWKfWVhRqh124tBhBjgK6lUtoBc1TA3H19/exec"

sync = SheetSync(webhook_post=POST_URL, webhook_get=GET_URL)
sync.sync()
