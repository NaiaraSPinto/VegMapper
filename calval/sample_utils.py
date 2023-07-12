import ee
import numpy as np
import pandas as pd
from scipy.stats import norm

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

    assert MOE_Algorithm in ["StehmanFoody", "Oloffson"],\
        "{} not found, use StehmanFoody or Oloffson.".format(MOE_Algorithm)
    
    if MOE_Algorithm == "StehmanFoody":
        print("Using Stehman Foody")
        anticipatedAcc = kwargs['anticipatedAcc']
        strata_df['p'] = anticipatedAcc
        d = MarginOfError
        strata_df['nh'] = ((z_score**2)*strata_df['p']*(1-strata_df['p']))/d**2
        strata_df['nh'] = strata_df['nh'].astype(int)
        strata_df['nh_adjusted'] = strata_df['nh']\
            .apply(lambda x: max(x, MinimumClassSample)) 

    elif MOE_Algorithm == "Oloffson":  
        print("Using Oloffson")
        CategoryOfInterest = kwargs['CategoryOfInterest']
        pct_area_cat_of_interest = strata_df.loc[strata_df['Cat'] == CategoryOfInterest, 'pct_area'].item()
        
        strata_df['qh'] = kwargs['mappingAcc']
        strata_df['wh'] =   strata_df['pct_area'] /100
        strata_df['SDh'] = (strata_df['qh'] * (1 - strata_df['qh']))**(1/2)
        strata_df['SDh_x_wh'] = strata_df['SDh'] * strata_df['wh']
          
        
        target_SE = MarginOfError * ((pct_area_cat_of_interest/100)/z_score)

        n = ((strata_df['SDh_x_wh'].sum())/target_SE)**2
        print('total sample size ', n)
        strata_df['nh'] = strata_df['wh'] * n ## NOTE THAT OLOFFSON METHOD distributes based on area
        strata_df['nh'] = strata_df['nh'].astype(int)
        strata_df['nh_adjusted'] = strata_df['nh']\
            .apply(lambda x: max(x, MinimumClassSample)) 
    
    return strata_df

    
def distribute_sample(totalSampleSizeAbsence,
                      totalSampleSizePresence,
                      absenceSampleWeights=None,
                      presenceSampleWeights=None):

    """
    Use the user specified absenceSampleWeights and presenceSampleWeights
    to divide totalSampleSize for absence and predence.
    return: two lists of sub-classes number, one for absence, the other for presence
    """

        
    if sum(absenceSampleWeights) != 1 or sum(presenceSampleWeights) != 1:
        print("absence or presence weight list does not sum to 1")
          
    else:
        print("distributing sample size for sub-classes...")
        sampleSizeAbsence = totalSampleSizeAbsence * np.array(absenceSampleWeights)
        sampleSizePresence = totalSampleSizePresence * np.array(presenceSampleWeights)
        
        sampleSizeAbsence = sampleSizeAbsence.astype(int).tolist()
        sampleSizePresence = sampleSizePresence.astype(int).tolist()
    return sampleSizeAbsence, sampleSizePresence