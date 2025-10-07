# run_desktop.py
from app import app
import threading, time, socket, os, sys, ctypes
import webview

def _run():
    app.run(host="127.0.0.1", port=5000, debug=False, use_reloader=False)

# sobe o backend
threading.Thread(target=_run, daemon=True).start()

# espera ficar de pé
for _ in range(100):
    try:
        with socket.create_connection(("127.0.0.1", 5000), timeout=0.2):
            break
    except OSError:
        time.sleep(0.1)
else:
    try:
        ctypes.windll.user32.MessageBoxW(0, "Falha ao iniciar o servidor local (porta 5000).", "Planilhex", 0x10)
    finally:
        sys.exit(1)

# força UI desktop (WebView2)
os.environ['PYWEBVIEW_GUI'] = 'edgechromium'
os.environ['PYWEBVIEW_EXPERIMENTAL_GUI'] = '0'

webview.create_window(
    "Planilhex",
    "http://127.0.0.1:5000",
    width=1200, height=800,
    resizable=True, frameless=False,
)
webview.start(gui='edgechromium', debug=False)
