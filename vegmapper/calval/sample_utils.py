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

    """
    Analyze the strata image.

    Args:
        strata_path (str): The path to the strata image in Google Earth Engine.

    Returns:
        list: A list containing:
            - strata (ee.Image): The strata image.
            - strata_df (pd.DataFrame): The category statistics.
            - misc (dict): A Python dictionary containing miscellaneous
              information such as minimum and maximum, category values,
              bounding box (bbox), and scale.
    
    """
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


def consolidate(strata_df, absenceCats, presenceCats):

    """
    Args:
        strata_df (pd.DataFrame): DataFrame with category counts.
        absenceCats (list): List of absence category IDs.
        presenceCats (list): List of presence category IDs.

    Returns:
        pd.DataFrame: Consolidated DataFrame with binary class counts, including
        the columns:
            - 'Cat': Binary class labels (0 for absence, 1 for presence).
            - 'pixel_ct': Total pixel count for each binary class.
            - 'pct_area': Relative shares of binary classes based on total area
              of interest (in percentage).
    """

    strata_df = strata_df.copy()
    strata_df = strata_df[strata_df['Cat'].isin(absenceCats + presenceCats)]

    # consolidate the pixel count as presence and abscence
    strata_df['Cat']=strata_df['Cat'].replace(absenceCats, 0)
    strata_df['Cat']=strata_df['Cat'].replace(presenceCats, 1)
    strata_df = strata_df.groupby('Cat')['pixel_ct'].sum().reset_index()

    # Calculate the relative shares of binary classes based on the total area of
    # interest.
    strata_df['pct_area'] = round(strata_df['pixel_ct']* 100/strata_df['pixel_ct'].sum(),
                                   3)
    
    return strata_df


def manual(strata_df, absenceSamples, presenceSamples):

    """
    Adjust strata DataFrame for binary classification.

    Args:
        strata_df (pd.DataFrame): DataFrame with category counts.
        absenceSamples (int): Number of absence samples.
        presenceSamples (int): Number of presence samples.

    Returns:
        pd.DataFrame: Adjusted DataFrame with binary class counts.
    """
    
    strata_df['nh_adjusted'] = [absenceSamples, presenceSamples]
    
    return strata_df

def automatic_requiredNumber(strata_df,
                             requiredNumberPresenceSamples,
                             estimatedPercentPresenceTarget,
                             requiredNumberAbsenceSamples,
                             estimatedAbsenceTarget):
    
    """
    Adjust strata DataFrame for binary classification based on required sample
    numbers.

    Args:
        strata_df (pd.DataFrame): DataFrame with category counts.
        requiredNumberPresenceSamples (int): Required number of presence samples.
        estimatedPercentPresenceTarget (float): Estimated percentage of presence
        target.
        requiredNumberAbsenceSamples (int): Required number of absence samples.
        estimatedAbsenceTarget (float): Estimated percentage of absence target.

    Returns:
        pd.DataFrame: Adjusted DataFrame with binary class counts.
    """
    
    strata_df['nh_adjusted'] = [round(requiredNumberAbsenceSamples/estimatedAbsenceTarget), 
                                round(requiredNumberPresenceSamples/estimatedPercentPresenceTarget)]
    
    return strata_df


# def automatic_StehmanFoody():


def automatic_moe(strata_df, MOE_Algorithm="StehmanFoody", **kwargs):
    """
    MarginOfError: or "d" is the desired half-width of the confidence interval
    anticipatedAcc: or "p" is the anticipated overall accuracy in the case of 
    simple random sampling, 
                    or anticipated user'saccuracy in the case of stratified
                    sampling where we determine 
                    the sample size nh for each stratum.
                    
    The Olofsson method is programmed largely based on this reference:                
    "https://www.openmrv.org/web/guest/w/modules/mrv/modules_3/sampling-design-for-estimation-of-area-and-map-accuracy"           
    
    qh: the proportion of stratum h that really falls in the target class (related
    to strata map accuracy).
    SDh: the standard deviation of stratum h, calculated from qh. 
    wh: strata weights, the number of pixels of a stratum to the total number of
    pixels in that study area.
        This is derived from the strata image.
    SDh_x_wh: SDh * wh, which facilitates the calculateion of n (see "3.3.1 Sample
    size" in the link above).
    
    targerSE: the target Standard Error, calculated from user specified 
    MarginOfError, ConfidenceLevel, and 
    
    """
    MarginOfError = kwargs['MarginOfError']
    ConfidenceLevel = kwargs['ConfidenceLevel']
    MinimumClassSample = kwargs['MinimumClassSample']
    

    #The z-score is calculated to account for the two-tailed nature of the
    #confidence interval
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
        pct_area_cat_of_interest = strata_df.loc[strata_df['Cat'] == CategoryOfInterest,
                                                  'pct_area'].item()
        
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
    Allocate the absence and presence samples into multiple categories (sub-classes),
    guided by the user-specified absenceSampleWeights and presenceSampleWeights. 
    For example, if presenceSampleWeights = [0.3, 0.7] and the presence sample 
    size is 100, then the presence samples will be split as [30, 70]. 
    
    return: three items: strata_df_mltcat with distributed samples, sampleSize 
    """

    if len(absenceCats) != len(absenceSampleWeights) or \
        len(presenceCats) != len(presenceSampleWeights):
        
        raise ValueError("Cat list must have the same length as its corresponding\
                          wweight list")
                
    if sum(absenceSampleWeights) != 1 or sum(presenceSampleWeights) != 1:
        print("Caution: absence or presence weight list does not sum to 1")
          
    else:
        
        totalSampleSizeAbsence=strata_df_bincat.loc[strata_df_bincat['Cat'] == 0,
                                                     'nh_adjusted'].values[0],
        totalSampleSizePresence=strata_df_bincat.loc[strata_df_bincat['Cat'] == 1,
                                                      'nh_adjusted'].values[0],
        
        print("distributing sample size for sub-classes...")
        sampleSizeAbsence = totalSampleSizeAbsence * np.array(absenceSampleWeights)
        sampleSizePresence = totalSampleSizePresence * np.array(presenceSampleWeights)
        
        sampleSizeAbsence = sampleSizeAbsence.astype(int).tolist()
        sampleSizePresence = sampleSizePresence.astype(int).tolist()
        
        cats_values = zip(presenceCats + absenceCats, sampleSizePresence + 
                          sampleSizeAbsence)
        
        # distribute
        for cat, value in cats_values:
            strata_df_mltcat.loc[strata_df_mltcat['Cat'] == cat, 'nh_final'] = value
            
        strata_df_mltcat = strata_df_mltcat.dropna(subset=['nh_final']).reset_index(drop=True)
        strata_df_mltcat['nh_final'] = strata_df_mltcat['nh_final'].astype(int)
        
    return strata_df_mltcat


def sampleFC_to_csv(sample_fc):

    """
    Convert Earth Engine FeatureCollection to a CSV DataFrame.

    Args:
        sample_fc (ee.FeatureCollection): Earth Engine FeatureCollection.

    Returns:
        pd.DataFrame: DataFrame containing sample data.
    """

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

    """
    Remove unwanted categories and set their sample counts to zero.

    Args:
        mltcat_old (pd.DataFrame): DataFrame with multi-class counts.

    Returns:
        pd.DataFrame: DataFrame with unwanted categories sample counts set to zero.
    """

    min_val = mltcat_old['Cat'].min()
    max_val = mltcat_old['Cat'].max()

    # Create a new DataFrame with the full sequence of numbers from min_val to max_val
    new_df = pd.DataFrame({'Cat': range(min_val, max_val + 1)})

    mltcat_new = pd.merge(new_df, mltcat_old, on='Cat', how='left')


    mltcat_new['nh_final'] = mltcat_new['nh_final'].fillna(0).astype(int)
    

    return mltcat_new