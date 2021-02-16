import os


class Cioppy:

    tmp_dir = "/tmp/"

    def log(self, level, msg):
        print(level, ":", msg)

    def copy(self, source, dst):
        print("CIOP copying file")
        return os.path.join("/tmp", source)

    def getparam(self, param):
        params = {
            "aoi": "POLYGON ((-98.5 18.9, -98.5 19.3, -99.2 19.3, -99.2 18.9, -98.5 18.9))",
            "start_date": "2020-08-01T00:00Z",
            "end_date": "2020-08-21T00:00Z"
        }

        return params.get(param, None)