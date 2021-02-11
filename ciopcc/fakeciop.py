import os


class Cioppy:

    tmp_dir = "/tmp/"

    def log(self, level, msg):
        print(level, ":", msg)

    def copy(self, source, dst):
        print("CIOP copying file")
        return os.path.join("/tmp", source)

