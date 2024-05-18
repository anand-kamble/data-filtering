import sys
from datetime import datetime
from typing import Literal

from termcolor import colored

sys.excepthook


class Test:
    """
    A class for writing and running tests with a fluent API.

    Attributes:
        title (str): The title of the test.
        mode (Literal["soft", "hard"]): The mode of the test. If set to "hard", the test will exit upon the first failure.
        export_file_name (str): The name of the file where the test results will be exported.
        result (str): The result of the test, formatted as a Markdown string.
        test_obj: The object that is currently being tested.

    Methods:
        expect(obj): Set the object to be tested.
        to_be(expected, msg: str | None = None): Assert that the test object is equal to the expected value.
        to_be_of_type(obj_type, msg: str | None = None): Assert that the test object is an instance of the expected type.
        to_be_approximately(expected, precision, msg: str | None = None): Assert that the test object is approximately equal to the expected value within a given precision.
        to_have_attribute(attribute_name: str | list[str], msg: str | None = None): Assert that the test object has the specified attribute(s).
        export_results(): Export the test results to a file.
    """

    def __init__(
        self,
        title: str,
        mode: Literal["soft", "hard"] = "soft",
        export_file_name: str = "test_results.md",
    ) -> None:
        """
        Initialize a Test object with a title.

        Args:
            title (str): The title of the test.
        """
        self.title = title
        self.mode = mode
        self.export_file_name = export_file_name
        self.result = f"# {self.title}  \n  "
        self.result += f"<p>Test started at {datetime.now()}</p>  \n --  "
        print(f"Running test - {self.title}:")

    def __success(self, message: str | None = None):
        """
        Print a success message for the current test.
        """
        self.result += f"\n\n- &check; {message}  "
        print(
            colored(
                (f"\t\u2714 {message}" if message else f"\u2714 {self.title}"),
                "green",
            )
        )

    def __failure(self, message: str | None = None):
        """
        Print a failure message for the current test.

        Args:
            message (str | None, optional): An optional message to include with the failure message.
        """
        self.result += f"\n\n- &cross; {message}  "
        print(
            colored(
                (f"\t\u2718 {message}" if message else f"\u2718 {self.title}"),
                "red",
            )
        )
        if self.mode == "hard":
            print("Exiting..")
            sys.exit(1)

    def __assert(self, condition: bool, message: str | None = None):
        """
        Assert a condition and print a success or failure message.

        Args:
            condition (bool): The condition to assert.
            message (str | None, optional): An optional message to include with the failure message.
        """
        if condition:
            self.__success(message)
        else:
            self.__failure(message)

    def expect(self, obj):
        """
        Set the object to be tested.

        Args:
            obj: The object to be tested.

        Returns:
            self: The Test object for method chaining.
        """
        self.test_obj = obj
        return self

    def to_be(self, expected, msg: str | None = None):
        """
        Assert that the test object is equal to the expected value.

        Args:
            expected: The expected value.

        Returns:
            self: The Test object for method chaining.
        """
        self.__assert(
            self.test_obj == expected,
            msg or f"Expected to be {expected}",
        )
        return self

    def to_be_of_type(self, obj_type, msg: str | None = None):
        """
        Assert that the test object is an instance of the expected type.

        Args:
            obj_type: The expected type.

        Returns:
            self: The Test object for method chaining.
        """
        self.__assert(
            isinstance(self.test_obj, obj_type),
            msg or f"Expected to be of type `{obj_type}`",
        )
        return self

    def to_be_approximately(self, expected, precision, msg: str | None = None):
        """
        Assert that the test object is approximately equal to the expected value within a given precision.
        Useful for testing floats.
        Args:
            expected: The expected value.
            precision: The maximum allowed difference between the test object and the expected value.

        Returns:
            self: The Test object for method chaining.
        """
        self.__assert(
            abs(self.test_obj - expected) < precision,
            msg
            or f"Expected to be approximately {expected} with precision {precision}",
        )
        return self

    def to_have_attribute(
        self, attribute_name: str | list[str], msg: str | None = None
    ):
        """
        Assert that the test object has the specified attribute(s).

        Args:
            attribute_name (str | list[str]): The name of the attribute(s) to check.

        Returns:
            self: The Test object for method chaining.

        Examples:
            >>> class MyClass:
            ...     def __init__(self):
            ...         self.attr1 = 1
            ...         self.attr2 = 2
            >>> obj = MyClass()
            >>> test = Test("MyClass")
            >>> test.expect(obj).to_have_attribute("attr1")
            [MyClass]: Passed
            >>> test.expect(obj).to_have_attribute(["attr1", "attr2"])
            [MyClass]: Passed
        """
        if isinstance(attribute_name, list):
            for attr in attribute_name:
                self.__assert(
                    hasattr(self.test_obj, attr),
                    f"Expected to have attribute `{attr}`",
                )

        else:
            self.__assert(
                hasattr(self.test_obj, attribute_name),
                msg or f"Expected to have attribute `{attribute_name}`",
            )

        return self

    def export_results(self):
        """
        Export the test results to a file.
        """
        with open(self.export_file_name, "w") as f:
            f.write(self.result)
