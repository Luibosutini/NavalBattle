from __future__ import annotations

import subprocess
import sys


def notify(title: str, message: str) -> None:
    """Send a desktop notification. Falls back to terminal bell + print on failure."""
    try:
        if sys.platform == "win32":
            _notify_windows(title, message)
        elif sys.platform == "darwin":
            _notify_macos(title, message)
        else:
            _notify_linux(title, message)
    except Exception:
        sys.stdout.write("\a")
        sys.stdout.flush()
        print(f"[NOTIFY] {title}: {message}")


def _notify_windows(title: str, message: str) -> None:
    t = title.replace("'", "\\'")
    m = message.replace("'", "\\'")
    ps = (
        "Add-Type -AssemblyName System.Windows.Forms;"
        "$n = New-Object System.Windows.Forms.NotifyIcon;"
        "$n.Icon = [System.Drawing.SystemIcons]::Information;"
        "$n.Visible = $true;"
        f"$n.ShowBalloonTip(8000, '{t}', '{m}', [System.Windows.Forms.ToolTipIcon]::Info);"
        "Start-Sleep -Milliseconds 500;"
        "$n.Dispose()"
    )
    subprocess.run(
        ["powershell", "-NonInteractive", "-Command", ps],
        check=False,
        timeout=10,
        capture_output=True,
    )


def _notify_macos(title: str, message: str) -> None:
    t = title.replace('"', '\\"')
    m = message.replace('"', '\\"')
    script = f'display notification "{m}" with title "{t}"'
    subprocess.run(["osascript", "-e", script], check=False, timeout=10, capture_output=True)


def _notify_linux(title: str, message: str) -> None:
    subprocess.run(
        ["notify-send", "--expire-time=8000", title, message],
        check=False,
        timeout=10,
        capture_output=True,
    )
