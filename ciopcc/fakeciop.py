import os


class Cioppy:

    tmp_dir = "/tmp/"

    def log(self, level, msg):
        print(level, ":", msg)

    def copy(self, source, dst, extract=True, credentials=None):
        print("CIOP copying file")
        return os.path.join("/tmp", source)

    def getparam(self, param):
        params = {
            "aoi": "POLYGON ((-98.5 18.9, -98.5 19.3, -99.2 19.3, -99.2 18.9, -98.5 18.9))",
            "start_date": "2020-08-01T00:00Z",
            "end_date": "2020-08-21T00:00Z"
        }

        return params.get(param, None)

    def search(self, end_point=None, params=None, output_fields=None, model=None, timeout=None):
        return [{
            'self': 'https://catalog.terradue.com/sentinel1/search?format=atom&uid=S1A_IW_GRDH_1SDV_20200820T122622_20200820T122647_033990_03F1CA_A0D5',
            'identifier': 'S1A_IW_GRDH_1SDV_20200820T122622_20200820T122647_033990_03F1CA_A0D5',
            'enclosure': 'https://store.terradue.com/download/sentinel1/files/v1/S1A_IW_GRDH_1SDV_20200820T122622_20200820T122647_033990_03F1CA_A0D5',
            'orbitDirection': 'DESCENDING',
            'track': '143',
            'polarisationChannels': 'VV VH',
            'productType': 'GRD',
            'startdate': '2020-08-20T12:26:22.5410000Z',
            'enddate': '2020-08-20T12:26:47.5400000Z',
            'wkt': 'POLYGON((-97.37886 17.348215,-97.067047 18.859432,-99.423454 19.286009,-99.714287 17.778299,-97.37886 17.348215))'
        }]

    def publish(self, result, mode=None):
        print("*** PUBLISH ** ", result)