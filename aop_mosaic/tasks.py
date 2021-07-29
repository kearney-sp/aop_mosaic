#Local Helper Functions
from .utils import get_obj, h5_zenith_dask, mapinfo_extents, get_corrected, hyobj_daskify

#Standard Python
import requests, datetime, re, os, json, glob
from typing import Optional, List

#Computing Stack
import xarray as xr
import numpy as np
from affine import Affine
from rasterio import Affine as r_Affine
import h5py
import stackstac

#Prefect
from prefect import task
import prefect

#Distributed / Parallel Computing
import dask
from dask import array as da
import ray

#Hyperspectral
import hytools as ht
from hytools.topo import calc_topo_coeffs
from hytools.brdf import calc_brdf_coeffs
from hytools.masks import mask_create


@task(max_retries=3, retry_delay=datetime.timedelta(seconds=3))
def query_data_urls(site: str, processDate: str) -> dict:
    prod_query = requests.get('https://data.neonscience.org/api/v0/products/DP1.30006.001').json()
    for urlsite in prod_query['data']['siteCodes']:
        if urlsite['siteCode'] == site:
            dates = np.array(urlsite['availableMonths'])
            urlsite.update(availableMonths=np.array(np.array(urlsite['availableMonths']))[np.isin(dates,processDate)])
            urlsite.update(availableDataUrls=np.array(np.array(urlsite['availableDataUrls']))[np.isin(dates,processDate)])
            return(urlsite)
    return({'res':'Fail'})

@task(max_retries=3, retry_delay=datetime.timedelta(seconds=3))
def query_file_urls(site_dict: dict,processDate: str, site: str) -> list:
    res = requests.get('https://data.neonscience.org/api/v0/data/DP1.30006.001/CPER/2017-05').json()['data']['files']
    f_list=[]
    for item in res:
        if item['name'][-14:] == 'reflectance.h5':
            f_list.append(item)
    return(f_list)

@task(max_retries=3, retry_delay=datetime.timedelta(seconds=3))
def BRDF_TOPO_Config(pipeline_dict: dict, site: str, processDate: str, cpus: int) -> dict:
    ######## FROM:https://github.com/EnSpec/hytools/blob/master/scripts/configs/image_correct_json_generate.py ########
    ######## Setup ########
    config_dict = {}

    ######## File Format/Type ########
    config_dict['bad_bands'] =[[300,400],[1337,1430],[1800,1960],[2450,2600]]
    config_dict['file_type'] = 'neon'
    images= [pipeline_dict['name']]
    images.sort()
    config_dict["input_files"] = images
    config_dict['export'] = {}
    config_dict['export']['coeffs']  = True
    config_dict['export']['image']  = True
    config_dict['export']['masks']  = True
    config_dict['export']['subset_waves']  = [660,550,440,850]
    config_dict['export']['output_dir'] = "./data1/temp/ht_test/"
    config_dict['export']["suffix"] = 'brdf'

    ######## Define Corrections ########
    config_dict["corrections"]  = ['topo','brdf']

    ######## Topographic Correction ########
    config_dict["topo"] =  {}
    config_dict["topo"]['type'] =  'scs+c'
    config_dict["topo"]['calc_mask'] = [["ndi", {'band_1': 850,'band_2': 660,
                                                 'min': 0.1,'max': 1.0}],
                                        ['ancillary',{'name':'slope',
                                                      'min': np.radians(5),'max':'+inf' }],
                                        ['ancillary',{'name':'cosine_i',
                                                      'min': 0.12,'max':'+inf' }],
                                        ['cloud',{'method':'zhai_2018',
                                                  'cloud':True,'shadow':True,
                                                  'T1': 0.01,'t2': 1/10,'t3': 1/4,
                                                  't4': 1/2,'T7': 9,'T8': 9}]]
    config_dict["topo"]['apply_mask'] = [["ndi", {'band_1': 850,'band_2': 660,
                                                 'min': 0.1,'max': 1.0}],
                                        ['ancillary',{'name':'slope',
                                                      'min': np.radians(5),'max':'+inf' }],
                                        ['ancillary',{'name':'cosine_i',
                                                      'min': 0.12,'max':'+inf' }]]
    config_dict["topo"]['c_fit_type'] = 'nnls'

    ######## BRDF ########
    #General Configs
    config_dict["brdf"]  = {}
    config_dict["brdf"]['solar_zn_type'] ='scene'

    #Flex Configs
    config_dict["brdf"]['type'] =  'flex'
    config_dict["brdf"]['grouped'] =  True
    config_dict["brdf"]['geometric'] = 'li_dense_r'
    config_dict["brdf"]['volume'] = 'ross_thick'
    config_dict["brdf"]["b/r"] = 2.5
    config_dict["brdf"]["h/b"] = 2
    config_dict["brdf"]['sample_perc'] = 0.1
    config_dict["brdf"]['interp_kind'] = 'linear'
    config_dict["brdf"]['calc_mask'] = [["ndi", {'band_1': 850,'band_2': 660,
                                                  'min': 0.1,'max': 1.0}],
                                        ['kernel_finite',{}],
                                        ['ancillary',{'name':'sensor_zn',
                                                      'min':np.radians(2),'max':'inf' }],
                                        ['neon_edge',{'radius': 30}],
                                        ['cloud',{'method':'zhai_2018',
                                                  'cloud':True,'shadow':True,
                                                  'T1': 0.01,'t2': 1/10,'t3': 1/4,
                                                  't4': 1/2,'T7': 9,'T8': 9}]]

    config_dict["brdf"]['apply_mask'] = [["ndi", {'band_1': 850,
                                                  'band_2': 660,
                                                  'min': 0.05,
                                                  'max': 1.0}]]
    
    #Flex Dynamic NDVI Configs
    config_dict["brdf"]['bin_type'] = 'dynamic'
    config_dict["brdf"]['num_bins'] = 18
    config_dict["brdf"]['ndvi_bin_min'] = 0.05
    config_dict["brdf"]['ndvi_bin_max'] = 1.0
    config_dict["brdf"]['ndvi_perc_min'] = 10
    config_dict["brdf"]['ndvi_perc_max'] = 95

    ######## Wavelength resampling options ########
    config_dict["resample"]  = False

    ######## Save Config to NamedTempfile ########
    config_dict['num_cpus'] = cpus
    pipeline_dict['BRDF_TOPO_CONFIG'] = config_dict
    return(pipeline_dict)

@task(max_retries=3, retry_delay=datetime.timedelta(seconds=3))
def download_file(pipeline_dict: dict, site: str, processDate: str, result_folder: str) -> dict:
    url = pipeline_dict['url']
    r = requests.get(url,stream=True)
    logger = prefect.context.get("logger")
    logger.info(r.headers)
    if result_folder[-1] != '/':
        result_folder = result_folder+'/'
    if os.path.isdir(result_folder+site+'_'+processDate) == False:
        os.mkdir(result_folder+site+'_'+processDate)
    local_url = result_folder+site+'_'+processDate+'/'+pipeline_dict['name']
    
    with open(local_url,mode='wb') as f:
        for chunk in r.iter_content(chunk_size=int(1e+8)):
            f.write(chunk)
    if r.status_code == 200:
        pipeline_dict['local_url'] = local_url
        return(pipeline_dict)
    else:
        pipeline_dict['local_url'] = None
        return(pipeline_dict)

@task
def get_file_meta(pipeline_dict: dict) -> dict:
    neon = ht.HyTools()
    neon.read_file(pipeline_dict['local_url'],file_type= 'neon')
    neon_header = neon.get_header()
    neon_header['wavelength'] = neon_header['wavelength'].tolist()
    pipeline_dict['file_meta'] = neon_header
    return(pipeline_dict)

@task
def write_pipeline_meta(pipeline_dict: list, site: str, processDate: str, result_folder: str) -> bool:
    if result_folder[-1] != '/':
        result_folder = result_folder+'/'
    with open(result_folder+site+'_'+processDate+'/neonAOP__DP130006_001__'+site+'__'+processDate+'.json',mode='w') as f:
        json.dump(pipeline_dict,f)
    return(True)

@task
def apply_corrections_mosaic(pipeline_dict: dict, site: str, processDate: str) -> dict:
    images = [pipeline_dict['local_url']]
    
    #Setup Ray
    if ray.is_initialized():
        ray.shutdown()

    ray.init(num_cpus = pipeline_dict['BRDF_TOPO_CONFIG']['num_cpus'],dashboard_port=9000)
    
    HyTools = ray.remote(ht.HyTools)
    actors = [HyTools.remote() for image in images]
    
    #Read File
    _ = ray.get([a.read_file.remote(image,pipeline_dict['BRDF_TOPO_CONFIG']['file_type']) for a,image in zip(actors,images)])
    
    #Apply Bad Bands
    _ = ray.get([a.create_bad_bands.remote(pipeline_dict['BRDF_TOPO_CONFIG']['bad_bands']) for a in actors])
    
    # apply the Corrections
    for correction in pipeline_dict['BRDF_TOPO_CONFIG']["corrections"]:
        if correction =='topo':
            calc_topo_coeffs(actors,pipeline_dict['BRDF_TOPO_CONFIG']['topo'])
        elif correction == 'brdf':
            calc_brdf_coeffs(actors,pipeline_dict['BRDF_TOPO_CONFIG'])
            
    #_ = ray.get([a.do.remote(apply_corrections,pipeline_dict) for a in actors])

    r = ray.get([a.do.remote(get_obj) for a in actors])[0]
    ray.shutdown()
    
    mapinfo = pipeline_dict['file_meta']['map info']
    af = r_Affine(float(mapinfo[5].strip()),
                0,
                float(mapinfo[3].strip()),
                0,
                -1.*float(mapinfo[6].strip()),
                float(mapinfo[4].strip()))
    
    pipeline_dict['hy_obj'] = {'shape':(r.lines,r.columns,len(r.wavelengths)),
                               'obj':r,
                               'transform':af}
    
    return(pipeline_dict)

@task
def pixel_mosaic_mask(pipeline_dict: dict, 
                      pipeline_list: list) -> dict:
    
    #Create Xarray object for flightline
    data,(ny,nx) = h5_zenith_dask(pipeline_dict['local_url'])
    mapinfo = pipeline_dict['file_meta']['map info']
    transform = pipeline_dict['hy_obj']['transform']
    xcoords,_ = Affine(*transform[0:6]) * (np.arange(nx)+0.5, np.arange(nx)+0.5)
    _,ycoords = Affine(*transform[0:6]) * (np.arange(ny)+0.5, np.arange(ny)+0.5)
    print(data.shape,len(xcoords),len(ycoords))
    ds = xr.DataArray(data=data,
                      coords={'x':xcoords,
                              'y':ycoords},
                      dims=('y','x'))
    ds = ds.compute()
    ds = ds.where(ds!=-9999.)
    
    #Create Dictionary of all the other xarray objects
    xr_obj = {}
    for pipe_dict in pipeline_list:
        if pipe_dict['name'] != pipeline_dict['name']:
            data,(ny,nx) = h5_zenith_dask(pipe_dict['local_url'])
            mapinfo = pipe_dict['file_meta']['map info']
            transform = pipe_dict['hy_obj']['transform']
            xcoords,_ = Affine(*transform[0:6]) * (np.arange(nx)+0.5, np.arange(nx)+0.5)
            _,ycoords = Affine(*transform[0:6]) * (np.arange(ny)+0.5, np.arange(ny)+0.5)
            print(data.shape,len(xcoords),len(ycoords))
            xr_obj[pipe_dict['local_url'].split('/')[-1][0:-3]] = xr.DataArray(data=data,
                                                                               coords={'x':xcoords,
                                                                                       'y':ycoords},
                                                                               dims=('y','x'))
    #Stack flights that overlap with flightline
    l = []
    for k in xr_obj.keys():
        print(k)
        x1 = xr_obj[k].sel(x=slice(ds.x.min(),
                                   ds.x.max()),
                           y=slice(ds.y.max(),
                                   ds.y.min()))
        
        if np.array(x1.shape).min()> 0:
            x1 = x1.interp_like(ds,method='linear',kwargs={'fill_value':np.nan})#
            l.append(x1.to_dataset(name=k))
    print(len(l))
    if len(l)>0:
        other_mins = xr.combine_by_coords(l)
        other_mins = other_mins.to_array(dim='flights').squeeze().drop('flights').compute()
        other_mins = other_mins.where(other_mins>0.,9999.)
        xr_mask = (ds<=other_mins) & (ds>0.)
    else:
        xr_mask = ds>0.
    xr_mask = xr_mask.to_dataset(name=pipeline_dict['local_url'].split('/')[-1][0:-3])
    pipeline_dict['mask'] = xr_mask
    return(pipeline_dict)

@task
def moasic_extent(pipeline_list: list) -> list:
    extents = []
    for pipe_dict in pipeline_list:
        extents.append(mapinfo_extents(pipe_dict['file_meta']))
    extents = np.array(extents)
    domain = [np.min(extents[:,0]),
              np.min(extents[:,1]),
              np.max(extents[:,2]),
              np.max(extents[:,3])]
    return(domain)

@task
def mosaic(pipeline_list: list, extents: list, site: str, processDate: str, result_folder: str) -> bool:
    t_file = pipeline_list[0] #template file to get common metadata
    wl = t_file['file_meta']['wavelength']
    x_coords = np.arange(extents[0],extents[2])+.5
    y_coords = np.arange(extents[1],extents[3])+.5
    t_h5 = h5py.File(t_file['local_url'],'r')['CPER']['Reflectance']['Reflectance_Data']
    nodata = t_h5.attrs['Data_Ignore_Value']
    #Update Mosaic with the mask corrected data
    ds_flts = []
    ds_flts_final = []
    for pipe_dict in pipeline_list:
        #Corrected (BRDF + Topo)
        h5_obj = h5py.File(pipe_dict['local_url'],'r')['CPER']['Reflectance']['Reflectance_Data']
        chnks = (h5_obj.chunks[0],h5_obj.chunks[1],h5_obj.shape[-1])
        shp = h5_obj.shape
        dtype = 'float64'
        minx,miny,maxx,maxy = mapinfo_extents(pipe_dict['file_meta'])
        wl = t_file['file_meta']['wavelength']
        x_coords = np.arange(minx,maxx)+.5
        y_coords = np.arange(miny,maxy)+.5
        data = da.from_array(hyobj_daskify(hy_obj = pipe_dict['hy_obj']['obj'],
                                           shape = pipe_dict['hy_obj']['shape'],
                                           ndims = 3,
                                           dtype = 'float32'),
                             chunks=chnks)
        da_tmp = xr.DataArray(data=data,
                              coords={'y':y_coords,
                                      'x':x_coords,
                                      'wl':wl},
                              dims=('y','x','wl'))

        #Apply Mask
        da_mask = pipe_dict['mask']
        mask = da.from_array(da_mask.to_array().squeeze().data,chunks=chnks[0:2])
        mask_3d = da.broadcast_to(mask[:,:,np.newaxis],data.shape,chunks=chnks)
        da_mask = xr.DataArray(data=mask_3d,
                               coords={'y':y_coords,
                                       'x':x_coords,
                                       'wl':wl},
                               dims=('y','x','wl'))
        ds_flts.append([da_tmp,da_mask])
        da_tmp = da_tmp.where(da_mask)#.to_dataset(name='ds_flt')
        
        
        ds_flts_final.append(da_tmp)
    mos = stackstac.mosaic(xr.concat(ds_flts_final,dim='ds_flt'),dim='ds_flt',axis=None)
    if result_folder[-1] != '/':
        result_folder = result_folder+'/'
    mos.to_zarr(result_folder+site+'_'+processDate+'/neonAOP__DP130006_001__'+site+'__'+processDate+'v3.zarr',
                mode = 'w',
                consolidated=True,
                encoding={'reflectance': {'dtype': 'float64',
                                          '_FillValue': -9999}})
    return(True)