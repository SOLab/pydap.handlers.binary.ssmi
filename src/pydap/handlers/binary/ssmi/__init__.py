"""A Pydap handler for binary SSMI files."""

import os
import csv
import re
import time
import copy
from stat import ST_MTIME
from email.utils import formatdate
import json

from pkg_resources import get_distribution

from pydap.model import *
from pydap.handlers.lib import BaseHandler
from pydap.handlers.helper import constrain
from pydap.exceptions import OpenFileError
#from pydap.parsers.das import add_attributes


class BinarySsmiHandler(BaseHandler):

    __version__ = get_distribution("pydap.handlers.binary.ssmi").version
    extensions = re.compile(r".*f[0-9]{2}_[0-9]{8}[0-9a-z]{2}[\.gz]?$", re.IGNORECASE)

    def __init__(self, filepath):
        BaseHandler.__init__(self)
        self.filename = os.path.split(filepath)[1]
        self.dataset = GridType(name=self.filename, attributes={
            "SSMI_GLOBAL" : {
                "CONVENTIONS" : "COARDS",
                "short_name" : "SSMIS",
                "long_name" : "Special Sensor Microwave Image Sounder",
                "producer_agency" : "Remote Sensing Systems",
                "product_version" : "Version-7",
                "spatial_resolution" : "0.25 degree",
                "temporal_resolution" : "Daily",
                "instrument" : "SSMIS",
                "original_filename" : self.filename,
            }
        })
        variables = [{
                'name' : 'time',
                'long_name' : 'Time',
                'add_offset' : 0,
                'scale_factor' : 6,
                '_FillValue' : 254,
                'units' : 'minutes' },
            {
                'name' : 'wspd',
                'long_name' : '10 meter Surface Wind Speed',
                'add_offset' : 0,
                'scale_factor' : 0.2,
                '_FillValue' : 254,
                'units' : 'm/sec'
            },
            {
                'name' : 'vapor',
                'long_name' : 'Atmospheric Water Vapor',
                'add_offset' : 0,
                'scale_factor' : 0.3,
                '_FillValue' : 254,
                'units' : 'mm'

            },
            {
                'name' : 'cloud',
                'long_name' : 'Cloud liquid Water',
                'add_offset' : -0.05,
                'scale_factor' : 0.01,
                '_FillValue' : 254,
                'units' : 'mm'

            },
            {
                'name' : 'rain',
                'long_name' : 'Rain Rate',
                'add_offset' : 0,
                'scale_factor' : 0.1,
                '_FillValue' : 254,
                'units' : 'mm/hr'
            },]
        for variable in variables:
            self.dataset[variable['name']] = BaseType(name=variable['name'],
                                              data=[range(1440),range(720),range(720*1440)],
                                              shape=(1440, 720, 2),
                                              dimensions=('lon', 'lat', 'part_of_day'),
                                              type=UInt16,
                                              attributes=({
                                                'long_name'     : variable['long_name'],
                                                'add_offset'    : variable['add_offset'],
                                                'scale_factor'  : variable['scale_factor'],
                                                '_FillValue'    : variable['_FillValue'],
                                                'units'         : variable['units']
                                              }))

    def parse_constraints(self, environ):
        return self.dataset
        #return constrain(self.dataset, environ.get('QUERY_STRING', ''))

if __name__ == "__main__":
    import sys
    from werkzeug.serving import run_simple

    #_test()

    application = BinarySsmiHandler(sys.argv[1])
    from pydap.wsgi.ssf import ServerSideFunctions
    application = ServerSideFunctions(application)
    run_simple('localhost', 8001, application, use_reloader=True)