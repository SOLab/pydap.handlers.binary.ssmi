"""A Pydap handler for binary SSMI files."""

import os
import re

import gzip

from pkg_resources import get_distribution
from pydap.model import *
from pydap.handlers.lib import BaseHandler
from pydap.handlers.helper import constrain
from pydap.lib import parse_qs, walk
from pydap.exceptions import OpenFileError
import numpy as np
import struct
import copy

class BinarySsmiHandler(BaseHandler):

    __version__ = get_distribution("pydap.handlers.binary.ssmi").version
    extensions = re.compile(r".*f[0-9]{2}_[0-9]{8}[0-9a-z]{2}(\.gz)?$", re.IGNORECASE)

    def __init__(self, filepath):
        BaseHandler.__init__(self)
        self.filepath = filepath
        self.filename = os.path.split(filepath)[1]
        self.dataset = DatasetType(name=self.filename, attributes={
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

        _dim = ('lon', 'lat', 'part_of_day')
        _shape = (1440, 720, 2)
        _type = UInt16

        self.variables = [
            BaseType(
                name='time',
                data=None,
                shape=_shape,
                dimensions=_dim,
                type=_type,
                attributes={
                    'long_name' : 'Time',
                    'add_offset' : 0,
                    'scale_factor' : 6,
                    '_FillValue' : 254,
                    'units' : 'minutes',
                    'coordinates': 'latitude longitude'
                }
            ),
            BaseType(
                name='wspd',
                data=None,
                shape=_shape,
                dimensions=_dim,
                type=_type,
                attributes={
                    'long_name' : '10 meter Surface Wind Speed',
                    'add_offset' : 0,
                    'scale_factor' : 0.2,
                    '_FillValue' : 254,
                    'units' : 'm/sec',
                }
            ),
            BaseType(
                name='vapor',
                data=None,
                shape=_shape,
                dimensions=_dim,
                type=_type,
                attributes=({
                    'long_name' : 'Atmospheric Water Vapor',
                    'add_offset' : 0,
                    'scale_factor' : 0.3,
                    '_FillValue' : 254,
                    'units' : 'mm',
                    'coordinates': 'latitude longitude'
                })
            ),
            BaseType(
                name='cloud',
                data=None,
                shape=_shape,
                dimensions=_dim,
                type=_type,
                attributes=({
                    'long_name' : 'Cloud liquid Water',
                    'add_offset' : -0.05,
                    'scale_factor' : 0.01,
                    '_FillValue' : 254,
                    'units' : 'mm',
                    'coordinates': 'latitude longitude'
                })
            ),

            BaseType(
                name='rain',
                data=None,
                shape=_shape,
                dimensions=_dim,
                type=_type,
                attributes=({
                    'long_name' : 'Rain Rate',
                    'add_offset' : 0,
                    'scale_factor' : 0.1,
                    '_FillValue' : 254,
                    'units' : 'mm/hr',
                    'coordinates': 'latitude longitude'
                })
            )]

        latVar = BaseType(
            name='lat',
            data=None,
            shape=(720,),
            dimensions=('lat',),
            type=Float32,
            attributes=({
                'long_name' : 'latitude',
                # 'add_offset' : 0,
                # 'scale_factor' : 1,
                'valid_range' : '-90, 90',
                'units' : 'degrees_north'
            })
        )

        lonVar = BaseType(
            name='lon',
            data=None,
            shape=(1440,),
            dimensions=('lon',),
            type=Float32,
            attributes=({
                'long_name' : 'longitude',
                # 'add_offset' : 0,
                # 'scale_factor' : 1,
                'valid_range' : '-180, 180',
                'units' : 'degrees_east'
            })
        )

        partVar = BaseType(
            name='part_of_day',
            data=None,
            shape=(2,),
            dimensions=('part_of_day',),
            type=UInt16,
            attributes=({
                'long_name' : 'part_of_day',
                # 'add_offset' : 0,
                # 'scale_factor' : 1,
                'valid_range' : '0, 1',
                'units' : 'part_of_day'
            })
        )

        self.dataset['lat'] = latVar
        self.dataset['lon'] = lonVar
        self.dataset['part_of_day'] = partVar

        for variable in self.variables:
            # variable_dict = variable.copy()
            # variable_dict.pop('name', None)
            # _dim = ('lon', 'lat', 'part_of_day')
            # _shape=(1440, 720, 2)
            # _type = UInt16
            #
            # if variable['name'] == 'lat':
            #     _dim = ('lat',)
            #     _shape = (720,)
            #     _type = Float32
            # elif variable['name'] == 'lon':
            #     _dim = ('lon',)
            #     _shape = (1440,)
            #     _type = Float32
            # elif variable['name'] == 'part_of_day':
            #     _dim = ('part_of_day',)
            #     _shape = (2,)
            print variable.name
            g = GridType(name=variable.name)
            g[variable.name] = variable

            g['lat'] = latVar.__deepcopy__()
            g['lon'] = lonVar.__deepcopy__()
            g['part_of_day'] = partVar.__deepcopy__()
            g.attributes = variable.attributes

            self.dataset[variable.name] = g
            self.dataset[variable.name].attributes = variable.attributes


    def parse_constraints(self, environ):
        projection, selection = parse_qs(environ.get('QUERY_STRING', ''))
        if projection:
            try:
                if self.filepath.endswith('gz'):
                    file = gzip.open(self.filepath, 'rb')
                else:
                    file = open(self.filepath, "rb")
                bytes_read = np.frombuffer(file.read(), np.uint8)
                file.close()
            except Exception, exc:
                message = 'Unable to open file %s: %s' % (self.filepath, exc)
                raise OpenFileError(message)

            for var in projection:
                import ipdb
                ipdb.set_trace()
                var_name = var[0][0]

                if var_name in ['lat', 'lon', 'part_of_day']:
                    if var_name == 'lat':
                        self.dataset['lat'].data = self.read_variable_lat()
                    elif var_name == 'lon':
                        self.dataset['lon'].data = self.read_variable_lon()
                    elif var_name == 'part_of_day':
                        self.dataset['part_of_day'].data = self.read_variable_part()
                else:
                    for variable in self.variables:
                        if variable.name == var_name:
                            slices = var[0][1]
                            if len(slices) != 3:
                                raise ValueError('Cannot obtain slices for %s. '
                                                 'Should be 3 slices, but %d found' % (var_name, len(slices)))
                            print 'retrieving %s' % var_name, slices
                            index = 0
                            for i in range(len(self.variables)):
                                if self.variables[i].name == variable.name:
                                    index = i
                            # import ipdb
                            # ipdb.set_trace()
                            print "==> "+var_name+"!!!!!!!!!!!!", variable.name
                            self.dataset[variable.name][variable.name].data = self.read_variable_data(bytes_read, index, slices)
                            print "=====>", len(self.dataset[variable.name][variable.name].data), self.dataset[variable.name][variable.name].data

                            self.dataset[variable.name]['lat'].data = self.read_variable_lat()
                            self.dataset[variable.name]['lon'].data = self.read_variable_lon()
                            self.dataset[variable.name]['part_of_day'].data = self.read_variable_part()

        # return self.dataset

        dataset_copy = copy.deepcopy(self.dataset)
        if projection:
            # val_arr = ['lat', 'lon', 'part_of_day']
            val_arr = []
            for proj in projection:
                val_arr.append(proj[0][0])
            print "val_arr = ", val_arr
            for key in dataset_copy.keys():
                if not key in val_arr:
                    dataset_copy.pop(key, None)
        # if projection:
        #     import ipdb
        #     ipdb.set_trace()
        return dataset_copy

    def read_variable_lat(self):
        latMin = 0
        latMax = 719

        # buf = np.empty(len(bytes), np.uint16)
        # cnt = 0

        # for i in range(1440):
        #     for j in range(720):
        #         for k in range(2):
        #             latValue = -89.875 + 0.25 * (latMin + j)
        #             # byteIndex = (1440 * j + i) + (1440 * 720 * 2) + (1440 * 720 * 5 * k)
        #             # print len(bytes), i, j, k, byteIndex
        #             if latValue > 100:
        #                 print "=========ERROR============"
        #             buf[cnt] = latValue
        #             cnt += 1

        buf = np.empty(719+1, np.float32)
        cnt = 0

        for j in range(720):
            latValue = -89.875 + 0.25 * (latMin + j)
            # byteIndex = (1440 * j + i) + (1440 * 720 * 2) + (1440 * 720 * 5 * k)
            # print len(bytes), i, j, k, byteIndex
            buf[cnt] = latValue
            cnt += 1

        return buf

    def read_variable_lon(self):
        lonMin = 0
        lonMax = 1439

        # buf = np.empty(len(bytes), np.uint16)
        # cnt = 0
        # for i in range(1440):
        #     for j in range(720):
        #         for k in range(2):
        #             lonValue = 0.25 * (lonMin + i)
        #             if lonValue > 180:
        #                 lonValue -= 180
        #             # print len(bytes), i, j, k, byteIndex
        #             buf[cnt] = lonValue
        #             cnt += 1

        buf = np.empty(lonMax+1, np.float32)
        cnt = 0
        for i in range(1440):
            lonValue = 0.25 * (lonMin + i)
            if lonValue > 180:
                lonValue -= 180
            # print len(bytes), i, j, k, byteIndex
            buf[cnt] = lonValue
            cnt += 1

        return buf

    def read_variable_part(self):
        buf = np.empty(2, np.uint16)
        cnt = 0
        for i in range(2):
            buf[cnt] = i
            cnt += 1
        return buf

    def read_variable_data(self, bytes, index, slices):
        # buf_len = (slices[0].stop - slices[0].start)*(slices[1].stop - slices[1].start)*(slices[2].stop - slices[2].start)
        buf = np.empty(len(bytes), np.uint16)
        cnt = 0
        for i in range(1440)[slices[0]]:
            for j in range(720)[slices[1]]:
                for k in range(2)[slices[2]]:
                    byteIndex = (1440 * j + i) + (1440 * 720 * index) + (1440 * 720 * 5 * k)
                    buf[cnt] = bytes[byteIndex]
                    cnt += 1
        return buf


if __name__ == "__main__":
    import sys
    from werkzeug.serving import run_simple

    #_test()

    application = BinarySsmiHandler(sys.argv[1])
    from pydap.wsgi.ssf import ServerSideFunctions
    application = ServerSideFunctions(application)
    run_simple('localhost', 8001, application, use_reloader=True)