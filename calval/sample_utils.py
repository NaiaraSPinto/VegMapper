import ee
import geemap
import numpy as np
import pandas as pd
from scipy.stats import norm


# Constant
QUAL_PALETTE = ['a6cee3', '1f78b4', 'b2df8a', '33a02c', 
                              'fb9a99', 'e31a1c', 'fdbf6f', 'ff7f00', 
                              'cab2d6', '6a3d9a', 'ffff99', 'b15928']


def analyze_strata_image(strata_path):
  
    # Retrive image from a GEE asset
    print("Retriving GEE image...")
    strata = ee.Image(strata_path)
    strataInfo = strata.getInfo()
    strata = strata.rename(['category'])
    
    # get metadata
    print("Accessing image metadata...")
    crs = strataInfo['bands'][0]['crs']
    scale = strata.projection().nominalScale().getInfo()
    bbox = strata.geometry().bounds().getInfo()['coordinates'][0]
    classBand = strata.bandNames().get(0).getInfo()
    
    # analyze categories
    print("Analyzing image categories...")
    strata_reduced = strata.reduceRegion(reducer = ee.Reducer.frequencyHistogram(),
                                         maxPixels = 9999999999)
    strata_hist = ee.Dictionary(strata_reduced).get(strata.bandNames().get(0)).getInfo()
    strata_df = pd.DataFrame(strata_hist.items(), columns=['Cat', 'pixel_ct'])
    strata_df['Cat'] = strata_df['Cat'].astype(int)
    total_area = strata_df['pixel_ct'].sum()
    
    cats = list(map(int,strata_hist.keys())) 
    cat_min = min(cats)
    cat_max = max(cats)
    print("Done!")
    
    return [strata, 
            strata_df,
            {"strata_hist":strata_hist,
             "total_area":total_area,
             "categories":cats,
             "category_min":cat_min,
             "category_max":cat_max,
             "classBand":classBand,
             "CRS": crs,
             "scale":scale,
             "bbox":bbox}]


def display(image, misc, palette=QUAL_PALETTE, zoom_level=7):
    #map_ = geemap.Map(center= map_center, zoom=zoom_level)
    map_ = geemap.Map()
    map_.centerObject(image, zoom_level)
    vizParamsStrata = {'min': misc['category_min'], 'max': misc['category_max'], 
                   'palette': palette}
    map_.addLayer(image, vizParamsStrata, "Strata")
    return map_

def display_samples(image, misc, samples_presence, samples_absence, sample_color=['FF0000','0000FF'],palette=QUAL_PALETTE):
    
    vizParamsPres = {"color": sample_color[0]} ## red
    vizParamsAbs = {"color": sample_color[1]} ## blue

    map_ = geemap.Map()
    #Specify the min and max labels and the color palette matching the labels.
    vizParamsStrata = {'min': misc['category_min'], 'max': misc['category_max'], 
                       'palette': palette}

    #print(vizParamsStrata)

    map_.addLayer(image, vizParamsStrata, "Strata")
    map_.centerObject(image)
    map_.addLayer(samples_presence, vizParamsPres, 'presence')
    map_.addLayer(samples_absence, vizParamsAbs, 'absence')
    map_.addLayerControl()
    return map_


def consolidate(strata_df, absenceCats, presenceCats):
    strata_df = strata_df.copy()
    strata_df = strata_df[strata_df['Cat'].isin(absenceCats + presenceCats)]

    # consolidate the pixel count as presence and abscence
    strata_df['Cat']=strata_df['Cat'].replace(absenceCats, 0)
    strata_df['Cat']=strata_df['Cat'].replace(presenceCats, 1)
    strata_df = strata_df.groupby('Cat')['pixel_ct'].sum().reset_index()

    # Calculate the relative shares of binary classes based on the total area of interest.
    strata_df['pct_area'] = round(strata_df['pixel_ct']* 100/strata_df['pixel_ct'].sum(), 3)
    
    return strata_df


def manual(strata_df, absenceSamples, presenceSamples):
    
    strata_df['nh_adjusted'] = [absenceSamples, presenceSamples]
    
    return strata_df

def automatic_requiredNumber(strata_df,
                             requiredNumberPresenceSamples,
                             estimatedPercentPresenceTarget,
                             requiredNumberAbsenceSamples,
                             estimatedAbsenceTarget):
    
    strata_df['nh_adjusted'] = [round(requiredNumberAbsenceSamples/estimatedAbsenceTarget), 
                                round(requiredNumberPresenceSamples/estimatedPercentPresenceTarget)]
    
    return strata_df


# def automatic_StehmanFoody():


def automatic_moe(strata_df, MOE_Algorithm="StehmanFoody", **kwargs):
    """
    MarginOfError: or "d" is the desired half-width of the confidence interval
    anticipatedAcc: or "p" is the anticipated overall accuracy in the case of simple random sampling, 
                    or anticipated user'saccuracy in the case of stratified sampling where we determine 
                    the sample size nh for each stratum.
                    
    The Olofsson method is programmed largely based on this reference:                
    "https://www.openmrv.org/web/guest/w/modules/mrv/modules_3/sampling-design-for-estimation-of-area-and-map-accuracy"           
    
    qh: the proportion of stratum h that really falls in the target class (related to strata map accuracy).
    SDh: the standard deviation of stratum h, calculated from qh. 
    wh: strata weights, the number of pixels of a stratum to the total number of pixels in that study area.
        This is derived from the strata image.
    SDh_x_wh: SDh * wh, which facilitates the calculateion of n (see "3.3.1 Sample size" in the link above).
    
    targerSE: the target Standard Error, calculated from user specified MarginOfError, ConfidenceLevel, and 
    
    """
    MarginOfError = kwargs['MarginOfError']
    ConfidenceLevel = kwargs['ConfidenceLevel']
    MinimumClassSample = kwargs['MinimumClassSample']
    

    #The z-score is calculated to account for the two-tailed nature of the confidence interval
    z_score = norm.ppf(1 - (1 - ConfidenceLevel) / 2)

    assert MOE_Algorithm in ["StehmanFoody", "Olofsson"],\
        "{} not found, use StehmanFoody or Olofsson.".format(MOE_Algorithm)
    
    if MOE_Algorithm == "StehmanFoody":
        print("Using Stehman Foody")
        anticipatedAcc = kwargs['anticipatedAcc']
        strata_df['p'] = anticipatedAcc
        d = MarginOfError
        strata_df['nh'] = ((z_score**2)*strata_df['p']*(1-strata_df['p']))/d**2
        strata_df['nh'] = strata_df['nh'].astype(int)
        strata_df['nh_adjusted'] = strata_df['nh']\
            .apply(lambda x: max(x, MinimumClassSample)) 

    elif MOE_Algorithm == "Olofsson":  
        print("Using Olofsson")
        CategoryOfInterest = kwargs['CategoryOfInterest']
        pct_area_cat_of_interest = strata_df.loc[strata_df['Cat'] == CategoryOfInterest, 'pct_area'].item()
        
        strata_df['qh'] = kwargs['mappingAcc']
        strata_df['wh'] =   strata_df['pct_area'] /100
        strata_df['SDh'] = (strata_df['qh'] * (1 - strata_df['qh']))**(1/2)
        strata_df['SDh_x_wh'] = strata_df['SDh'] * strata_df['wh']
          
        
        target_SE = MarginOfError * ((pct_area_cat_of_interest/100)/z_score)

        n = ((strata_df['SDh_x_wh'].sum())/target_SE)**2
        print('total sample size ', n)
        strata_df['nh'] = strata_df['wh'] * n ## NOTE THAT OLOFSSON METHOD distributes based on area
        strata_df['nh'] = strata_df['nh'].astype(int)
        strata_df['nh_adjusted'] = strata_df['nh']\
            .apply(lambda x: max(x, MinimumClassSample)) 
    
    return strata_df

    
def distribute_sample(strata_df_bincat,
                      strata_df_mltcat,
                      absenceCats,
                      presenceCats,
                      absenceSampleWeights,
                      presenceSampleWeights):

    """
    Allocate the absence and presence samples into multiple categories (sub-classes), guided by 
    the user-specified absenceSampleWeights and presenceSampleWeights. 
    For example, if presenceSampleWeights = [0.3, 0.7] and the presence sample size is 100, 
    then the presence samples will be split as [30, 70]. 
    
    return: three items: strata_df_mltcat with distributed samples, sampleSize 
    """

    if len(absenceCats) != len(absenceSampleWeights) or \
        len(presenceCats) != len(presenceSampleWeights):
        
        raise ValueError("Cat list must have the same length as its corresponding wweight list")
                
    if sum(absenceSampleWeights) != 1 or sum(presenceSampleWeights) != 1:
        print("Caution: absence or presence weight list does not sum to 1")
          
    else:
        
        totalSampleSizeAbsence=strata_df_bincat.loc[strata_df_bincat['Cat'] == 0, 'nh_adjusted'].values[0],
        totalSampleSizePresence=strata_df_bincat.loc[strata_df_bincat['Cat'] == 1, 'nh_adjusted'].values[0],
        
        print("distributing sample size for sub-classes...")
        sampleSizeAbsence = totalSampleSizeAbsence * np.array(absenceSampleWeights)
        sampleSizePresence = totalSampleSizePresence * np.array(presenceSampleWeights)
        
        sampleSizeAbsence = sampleSizeAbsence.astype(int).tolist()
        sampleSizePresence = sampleSizePresence.astype(int).tolist()
        
        cats_values = zip(presenceCats + absenceCats, sampleSizePresence + sampleSizeAbsence)
        
        # distribute
        for cat, value in cats_values:
            strata_df_mltcat.loc[strata_df_mltcat['Cat'] == cat, 'nh_final'] = value
            
        strata_df_mltcat = strata_df_mltcat.dropna(subset=['nh_final']).reset_index(drop=True)
        strata_df_mltcat['nh_final'] = strata_df_mltcat['nh_final'].astype(int)
        
    return strata_df_mltcat


def sampleFC_to_csv(sample_fc):
    features = sample_fc.toList(sample_fc.size())

    # Extract lat and lon properties from each feature
    def extract_lat_lon(feature):
        lat = ee.Feature(feature).geometry().coordinates().get(1)
        lon = ee.Feature(feature).geometry().coordinates().get(0)
        cat = ee.Feature(feature).get('category')
        return {'LAT': lat, 'LON': lon, 'STRATA_CAT':cat}

    # Map the extraction function over the feature list
    lat_lon_list = features.map(extract_lat_lon)
    df = pd.DataFrame(lat_lon_list.getInfo())
    
    # add additional CEO columns
    df['PLOTID'] = range(1, len(df) + 1)
    
    
    return df

def unwant_cat_samples_zero(mltcat_old):
    min_val = mltcat_old['Cat'].min()
    max_val = mltcat_old['Cat'].max()

    # Create a new DataFrame with the full sequence of numbers from min_val to max_val
    new_df = pd.DataFrame({'Cat': range(min_val, max_val + 1)})

    mltcat_new = pd.merge(new_df, mltcat_old, on='Cat', how='left')


    mltcat_new['nh_final'] = mltcat_new['nh_final'].fillna(0).astype(int)

    return mltcat_new