"""Small launcher kept separate from the application module."""
import sys as _sys

# ---------------------------------------------------------------------------
# بوت‌استرپِ UTF-8 (ویندوز) — باید قبل از هر import دیگه‌ای اجرا بشه
# ---------------------------------------------------------------------------
# روی ویندوز، وقتی خروجی به فایل/لوله ریدایرکت می‌شه، پایتون انکودینگ لوکال
# (cp1252 و مشابه) رو انتخاب می‌کنه؛ بعدش هر print/log فارسی یا ایموجی با
# UnicodeEncodeError می‌افته و پروسه می‌میره. اینجا جریان‌های استاندارد روی
# UTF-8 با errors="replace" ست می‌شن تا حتی یک پیام هم باعث کرش نشه.
for _stream_name in ("stdin", "stdout", "stderr"):
    _stream = getattr(_sys, _stream_name, None)
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
del _stream_name, _stream

from main import run_forever

if __name__ == "__main__":
    run_forever()
