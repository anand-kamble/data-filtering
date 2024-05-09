from colorama import Fore


class CopaError(Exception):
    """
    Custom exception class for handling errors in the Copa application.

    This class extends the base Exception class and modifies the error message to be displayed in red color.

    Attributes:
        message (str): The error message to be displayed when the exception is raised.
    """

    def __init__(self, message):
        """
        Initialize the CopaError with a custom error message.

        Args:
            message (str): The error message to be displayed when the exception is raised.
        """
        super().__init__(Fore.RED + message + Fore.RESET)
