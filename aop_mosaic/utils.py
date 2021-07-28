import h5py
import dask
from dask import array as da


#Helper
def get_obj(hy_obj):
    return(hy_obj)

def h5_zenith_dask(file: str) -> dask.array.core.Array:
    h = h5py.File(file,'r')['CPER']['Reflectance']['Metadata']['to-sensor_Zenith_Angle']
    a = dask.array(h)
    return(a,h.shape)

def mapinfo_extents(meta: str) -> str:
    x_min = float(meta['map info'][3].strip())
    y_max = float(meta['map info'][4].strip())
    width = float(meta['samples'])
    length = float(meta['lines'])
    xres = float(meta['map info'][5].strip())
    yres = float(meta['map info'][6].strip())
    x_max = x_min + (width * xres)
    y_min = y_max - (length * yres)
    return([x_min,y_min,x_max,y_max])

def get_corrected(hy_obj):
    return(hy_obj.get_chunk(0,99999,0,999999,corrections=['brdf','topo'],resample=False))

class hyobj_daskify(object):
    def __init__(self,hy_obj,shape,dtype,ndims):
        self._hy_obj = hy_obj
        self.shape = shape
        self.dtype = dtype
        self.ndim = ndims
    def __getitem__(self,s):
        return(self._hy_obj.get_chunk(s[0].start,s[0].stop,s[1].start,s[1].stop,corrections=['brdf','topo'],resample=False).transpose((1,0,2)))
