import io
import unittest
from unittest.mock import patch
import launch_console


class ConsoleLauncherTests(unittest.TestCase):
    def test_open_existing_paper_console_without_second_worker(self):
        with patch('launch_console.urlopen',return_value=io.BytesIO(b'<meta name="aegis-paper-workspace" content="isolated">')),patch('launch_console.webbrowser.open') as browser,patch('launch_console.subprocess.call') as process:
            self.assertEqual(launch_console.main(),0)
        browser.assert_called_once_with('http://127.0.0.1:8778/');process.assert_not_called()

    def test_other_app_on_port_is_not_claimed_or_replaced(self):
        with patch('launch_console.urlopen',return_value=io.BytesIO(b'<title>Another app</title>')),patch('launch_console.webbrowser.open') as browser,patch('launch_console.subprocess.call') as process:
            self.assertEqual(launch_console.main(),1)
        browser.assert_not_called();process.assert_not_called()
