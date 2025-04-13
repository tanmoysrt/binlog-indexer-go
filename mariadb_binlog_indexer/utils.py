import platform
import os


def find_shared_library(self):
    """Find and load the appropriate library."""
    if platform.system() != "Linux":
        raise RuntimeError("This package only supports Linux")

    machine = platform.machine().lower()
    if machine == "x86_64" or machine == "amd64":
        lib_name = "indexer_linux_amd64"
    elif machine == "aarch64" or machine == "arm64":
        lib_name = "mariadb_binlog_indexer_linux_arm64.so"
    else:
        raise RuntimeError(f"Unsupported Linux architecture: {machine}")

    package_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(package_dir, "lib", lib_name)
