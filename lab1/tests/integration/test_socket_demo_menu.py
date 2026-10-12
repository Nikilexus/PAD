import io
import sys
import unittest
from unittest.mock import patch

from scripts.run_socket_demo import main


class SocketDemoMenuTest(unittest.TestCase):
    def test_send_all_messages_menu_option_publishes_from_all_publishers(self) -> None:
        answers = iter(
            [
                "1",  # tcp
                "6",  # send all messages
                "1,2",  # billing and warehouse receivers
                "1",  # no disconnections
            ]
        )
        output = io.StringIO()

        with patch.object(sys, "argv", ["run_socket_demo.py"]):
            with patch("builtins.input", lambda prompt="": next(answers)):
                with patch("sys.stdout", output):
                    main()

        text = output.getvalue()
        self.assertIn("web-publisher->order.created", text)
        self.assertIn("support-publisher->order.cancelled", text)
        self.assertIn("billing-consumer processed: ORD-SOCKET-DEMO-1", text)
        self.assertIn("warehouse-consumer processed: ORD-SOCKET-DEMO-1", text)
        self.assertEqual(text.count("PUBLISH message="), 5)
        self.assertNotIn("error:", text)


if __name__ == "__main__":
    unittest.main()
