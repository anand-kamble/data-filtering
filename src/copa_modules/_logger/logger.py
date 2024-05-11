import datetime

from colorama import Fore


class Logger:
    """
    A simple logger for writing messages to a log file.

    Attributes:
        log_file (str): The name of the log file to write to.
        scope (str): The scope in which the logger is used, to be included in log messages.
    """

    def __init__(self, scope: str, log_file="copa_logs.txt"):
        """
        Initialize a new Logger.

        Args:
            scope (str): The scope in which the logger is used.
            log_file (str, optional): The name of the log file to write to. Defaults to "copa_logs.txt".
        """
        self.log_file = log_file
        self.scope: str = scope

    def log(
        self,
        message,
        print_message: bool = False,
    ):
        """
        Write a message to the log file.

        Args:
            message (str): The message to write.
        """
        log_msg = Fore.LIGHTBLUE_EX + message + Fore.RESET
        if print_message:
            print(log_msg)
        with open(self.log_file, "a") as f:
            f.write(f"[{datetime.datetime.now()}] ({self.scope}) {message}\n")
