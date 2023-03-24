import logging
import os

from dotenv import load_dotenv

logging_dict = {
    "critical": logging.CRITICAL,
    "error": logging.ERROR,
    "warning": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG,
}

load_dotenv(verbose=True)


class EnvironmentVariableNotSet(Exception):
    pass


class ENV:
    def __init__(self) -> None:
        """Add environment variables here"""
        self.LOGGING_LEVEL = logging_dict[os.getenv("LOGGING_LEVEL")]

    def check_environment_variable(self):
        for property in dir(self):
            if (
                (getattr(self, property) is None)
                and (not property.startswith("__"))
                and (property.isupper())
            ):
                print("LOADING ENVIRONMENT VARIABLE: FAILED!")
                raise EnvironmentVariableNotSet(
                    f"SET {property} AS AN ENVIRONMENT VARIABLE!"
                )
        print("LOADING ENVIRONMENT VARIABLE: SUCEEDED!")


env = ENV()
env.check_environment_variable()
