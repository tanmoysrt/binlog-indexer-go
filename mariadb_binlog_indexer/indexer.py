import os
import platform
import subprocess
import duckdb


class Indexer:
    def __init__(self, base_path: str, db_name: str):
        self.indexer_lib = self._find_indexer_lib_executable()
        self.base_path = base_path
        self.db_name = db_name

    def add(self, binlog_path: str, batch_size: int = 10000):
        subprocess.run(
            [
                self.indexer_lib,
                "add",
                self.base_path,
                binlog_path,
                self.db_name,
                str(batch_size),
            ]
        )

    def remove(self, binlog_path: str):
        subprocess.run(
            [self.indexer_lib, "remove", self.base_path, binlog_path, self.db_name]
        )

    def _find_indexer_lib_executable(self):
        if platform.system() != "Linux":
            raise RuntimeError("This package only supports Linux")

        machine = platform.machine().lower()
        if machine == "x86_64" or machine == "amd64":
            lib_name = "indexer_linux_arm64"
        elif machine == "aarch64" or machine == "arm64":
            lib_name = "indexer_linux_arm64"
        else:
            raise RuntimeError(f"Unsupported Linux architecture: {machine}")

        package_dir = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(package_dir, "lib", lib_name)

    def _get_db(self):
        if self._db is not None:
            return self._db
        self._db = duckdb.connect(
            database=os.path.join(self.base_path, self.db_name), read_only=True
        )
        return self._db
