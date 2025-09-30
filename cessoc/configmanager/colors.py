import logging
import sys
import platform
import os


class Colors:
    """
    ANSI color codes
    https://gist.github.com/rene-d/9e584a7dd2935d0f461904b9f2950007
    """

    BLACK = "\033[0;30m"
    RED = "\033[0;31m"
    GREEN = "\033[0;32m"
    BROWN = "\033[0;33m"
    BLUE = "\033[0;34m"
    PURPLE = "\033[0;35m"
    CYAN = "\033[0;36m"
    LIGHT_GRAY = "\033[0;37m"
    DARK_GRAY = "\033[1;30m"
    LIGHT_RED = "\033[1;31m"
    LIGHT_GREEN = "\033[1;32m"
    YELLOW = "\033[1;33m"
    LIGHT_BLUE = "\033[1;34m"
    LIGHT_PURPLE = "\033[1;35m"
    LIGHT_CYAN = "\033[1;36m"
    LIGHT_WHITE = "\033[1;37m"
    BOLD = "\033[1m"
    FAINT = "\033[2m"
    ITALIC = "\033[3m"
    UNDERLINE = "\033[4m"
    BLINK = "\033[5m"
    NEGATIVE = "\033[7m"
    CROSSED = "\033[9m"
    END = "\033[0m"

    def __init__(self):
        """Check if a supported tty is being used"""
        super().__init__()
        self.colors_supported = False

        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        handle = sys.stdout
        if (hasattr(handle, "isatty") and handle.isatty()) or ("TERM" in os.environ and os.environ["TERM"] == "ANSI"):
            if platform.system() == "Windows" and not ("TERM" in os.environ and os.environ["TERM"] == "ANSI"):
                self.logger.info("Windows console, no ANSI color support")
                self.colors_supported = False
            else:
                self.logger.info("ANSI color output enabled")
                self.colors_supported = True
        else:
            self.colors_supported = False
            self.logger.info("Unsupported tty, ANSI color output disabled")

    def color(self, string, *codes):
        """Format string with codes if colors are supported"""
        if self.colors_supported:
            return "{}{}{}".format("".join(codes), string, Colors.END)
        else:
            return string