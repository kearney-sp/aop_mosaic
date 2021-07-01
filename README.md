
## UNDER DEVELOPMENT

---
### Overview

Data workflow automation (via [prefect](https://www.prefect.io/)) to process NEON hyperspectral data into a single mosaic.

1. Download NEON AOP hyperspectral data (DP130006_001)
2. Aggregate metadata file (.json) for the data
3. Apply BRDF and Topographic corrections using [HyTools](https://github.com/EnSpec/hytools)
4. Mosaic flights by selecting pixels with the sensor zenith angle nearest to NADIR
5. Write data to a [Zarr](https://zarr.readthedocs.io/en/stable/tutorial.html#usage-tips) file (via [xarray](http://xarray.pydata.org/en/stable/generated/xarray.Dataset.to_zarr.html))

### Implementation

1. Create a prefect account (Free)
2. Setup Prefect Cloud and Agent environment (see here - https://docs.prefect.io/orchestration/tutorial/overview.html)
3. Setup cloud project and register flow (see here - https://docs.prefect.io/orchestration/tutorial/first.html)
4. Activate flow (configure as necassary) on prefect cloud UI.

### Examples

Runs On: [data_science_im_rs:latest](https://hub.docker.com/layers/rowangaffney/data_science_im_rs/latest/images/sha256-bcb165314a8fc41b0a6413d2bbb491be74cdb24d625a82d3ac90951ee6902d3b?context=repo)

See: https://github.com/rmg55/aop_mosaic/blob/main/Neon_AOP_flow.ipynb

### Todo

1. Change cluster scaling from static to adaptive
2. Improve performance on mosaic
3. Contact Phil Townsend Lab about BRDF parameters/configurations
