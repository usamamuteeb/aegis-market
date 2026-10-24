"""Open an existing isolated console or launch one; never duplicate its worker."""
import subprocess
import sys
import webbrowser
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

URL='http://127.0.0.1:8778/'

def main():
    try:
        with urlopen(URL,timeout=3) as response:
            html=response.read(100_000)
    except URLError:
        root=Path(__file__).resolve().parent
        return subprocess.call([sys.executable,str(root/'intelligence_cli.py'),'--workspace',str(root/'research-workspace'),'serve','--port','8778','--open-browser'],cwd=root)
    if b'name="aegis-paper-workspace" content="isolated"' not in html:
        print('Port 8778 belongs to an unrecognized app; no worker started.')
        return 1
    webbrowser.open(URL)
    print('Opened the existing Aegis paper console. No duplicate worker started.')
    return 0

if __name__=='__main__':raise SystemExit(main())
