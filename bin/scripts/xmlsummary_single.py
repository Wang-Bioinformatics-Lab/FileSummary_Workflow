import argparse
from lxml import etree
import obonet
from pyteomics import mgf
import networkx
import csv
import os
import sys


HEADERS = ["Filename",
            "Original_Path",
            "Ion_Source",
            "Mass_Analyzer",
            "Mass_Detector",
            "Model",
            "Vendor",
            "MS1s",
            "MS2s",
            "MS3s",
            "MS4s",
            "MS5s",
            "MS6s",
            "MS7s",
            "MS8s",
            "MS9s",
            "MS10+"] 

def process_MGF(input_file, original_path, output_filename):
    parsed_mgf = mgf.read(open(input_file, 'r'))
    output_dictionary = {}
    output_dictionary['Filename'] = os.path.basename(input_file)
    output_dictionary['Original_Path'] = original_path
    output_dictionary['Ion_Source'] = ''
    output_dictionary['Mass_Analyzer'] = ''
    output_dictionary['Mass_Detector'] = ''
    output_dictionary['Model'] = ''
    output_dictionary['Vendor'] = ''
    
    # Iterate over the scans and count the number of MS levels
    ms_level_counts = [0]*10
    for scan in parsed_mgf:
        scan_ms_level = int(scan['params']['mslevel'])
        if scan_ms_level >= 10:
            ms_level_counts[-1] += 1
        else:
            ms_level_counts[int(scan['mslevel'])-1] += 1
            
    for mslevel in range(1,10):
        output_dictionary[f"MS{mslevel}s"] = ms_level_counts[mslevel-1]
    output_dictionary["MS10+"] = ms_level_counts[-1]
    
    with open(output_filename, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=HEADERS, delimiter='\t')
        writer.writeheader()
        writer.writerow(output_dictionary)

def process_mzXML(input_file, original_path, output_filename):
    lxml_tree = etree.parse(input_file)
    output_dictionary = {}
    output_dictionary['Filename'] = os.path.basename(input_file)
    output_dictionary['Original_Path'] = original_path
    
    runs = lxml_tree.findall("//{*}msRun")
    if len(runs) > 1:
        raise NotImplementedError("Multiple runs not supported")
    lxml_run = runs[0]
    
    ############## Parse Instrument Information ##################
    instrument_configuration_list = lxml_run.findall('.//{*}msInstrument')
    instrumentConfigurationDict = {}
    
    for config in instrument_configuration_list:
        config_id = str(config.get('msInstrumentID'))
        instrumentConfigurationDict[config_id] = {}
        # Want: scan_precursor_analyzer, scan_precursor_ionization, scan_precursor_detector, scan_precursor_instrument_vendor, scan_precursor_model
        # Get all lxml elements:
        msAnalyzer      = config.findall('.//{*}msMassAnalyzer')
        msIonization    = config.findall('.//{*}msIonisation')
        msDetector      = config.findall('.//{*}msDetector')
        msVendor        = config.findall('.//{*}msManufacturer')
        msModel         = config.findall('.//{*}msModel')
        
        # Get names for each element:
        msAnalyzer      = [x.get('value') for x in msAnalyzer]
        msIonization    = [x.get('value') for x in msIonization]
        msDetector      = [x.get('value') for x in msDetector]
        msVendor        = [x.get('value') for x in msVendor]
        msModel         = [x.get('value') for x in msModel]

        if len(msIonization) > 0 :
            instrumentConfigurationDict[config_id]['source']  = msIonization
        if len(msAnalyzer) > 0 :
            instrumentConfigurationDict[config_id]['analyzer'] = msAnalyzer
        if len(msDetector) > 0 :
            instrumentConfigurationDict[config_id]['detector'] = msDetector
        if len(msVendor) == 1:
            instrumentConfigurationDict[config_id]['vendor'] = [msVendor[0]]
        if len(msVendor) > 1:
            raise ValueError("Multiple instrument vendors not supported.")
        if len(msModel) == 1:
            instrumentConfigurationDict[config_id]['model'] = [msModel[0]]
        elif len(msModel) > 1:
            raise ValueError("Multiple instrument models not supported.")
        
    for key in instrumentConfigurationDict.keys():
        for field in instrumentConfigurationDict[key].keys():
            instrumentConfigurationDict[key][field] = ';'.join(instrumentConfigurationDict[key][field])
    # Join together each field by '|'. Note that the numner of '|' will be the same for each field, regardless of whether there is data
    output_dictionary['Ion_Source'] = '|'.join([instrumentConfigurationDict[x].get('source', '') for x in instrumentConfigurationDict.keys()])
    output_dictionary['Mass_Analyzer'] = '|'.join([instrumentConfigurationDict[x].get('analyzer', '') for x in instrumentConfigurationDict.keys()])
    output_dictionary['Mass_Detector'] = '|'.join([instrumentConfigurationDict[x].get('detector', '') for x in instrumentConfigurationDict.keys()])
    output_dictionary['Model'] = '|'.join([instrumentConfigurationDict[x].get('model', '') for x in instrumentConfigurationDict.keys()])
    output_dictionary['Vendor'] = '|'.join([instrumentConfigurationDict[x].get('vendor', '') for x in instrumentConfigurationDict.keys()])

    ############## Parse Spectrum Information ##################
    all_spectra = lxml_run.findall('.//{*}scan')
    all_ms_levels = [x.get('msLevel') for x in all_spectra] # List of ms levels as type str
    # Get counts up to MS 9 and 10+
    ms_level_counts = [all_ms_levels.count(str(x)) for x in range(1,10)] + [sum([int(x) > 9 for x in all_ms_levels])]
    for mslevel in range(1,10):
        output_dictionary[f"MS{mslevel}s"] = ms_level_counts[mslevel-1]
    output_dictionary["MS10+"] = ms_level_counts[-1]
    
    with open(output_filename, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=HEADERS, delimiter='\t')
        writer.writeheader()
        writer.writerow(output_dictionary)

def process_mzML(input_file, original_path, output_filename, ontology_file):
    lxml_tree = etree.parse(input_file)
    
    output_dictionary = {}
    output_dictionary['Filename'] = os.path.basename(input_file)
    output_dictionary['Original_Path'] = original_path
    
    ############## Parse Ontology Network ##################
    graph = obonet.read_obo(str(ontology_file))
    # Get all ids
    model_ids = networkx.ancestors(graph, 'MS:1000031')
    instrument_vendor_ids = ['MS:1001269'] # Seems that this is not always used
   
    # Convert to names
    id_to_name = {id_: data.get('name') for id_, data in graph.nodes(data=True)}
    model_names = [id_to_name[x] for x in model_ids]
    instrument_vendor_names = [id_to_name[x] for x in instrument_vendor_ids]
    
    ############## Parse Instrument Information ##################
    # Create a dictionary of the values contained in each referencable param group
    referenceableParamGroupList = lxml_tree.findall(".//{*}referenceableParamGroupList//{*}referenceableParamGroup")
    referenceableParamGroupDict = {}
    for referenceableParamGroup in referenceableParamGroupList:
        key = referenceableParamGroup.get('id')
        referenceableParamGroupDict[key] = {}
        for param in referenceableParamGroup:
            referenceableParamGroupDict[key][param.get('name')] = param.get('value')
    
    instrument_configs = [x.findall(".//{*}instrumentConfiguration") for x in lxml_tree.findall("//{*}instrumentConfigurationList")]
    # Flatten
    instrument_configs = [item for sublist in instrument_configs for item in sublist]

    instrumentConfigurationDict = {}

    for config in instrument_configs:
        key = config.get('id')
        instrumentConfigurationDict[key] = {}
        ############## Looking for Model/Vendor ##################
        #### Check if there is a referenceableParamGroupRef
        # Will yield a list of dicts where each dict is cvParam_name: value
        referencableParamGroupList = [referenceableParamGroupDict[x.get('ref')] for x in config.findall(".//{*}referenceableParamGroupRef")]
        
        referencedParamNames = [x.keys() for x in referencableParamGroupList]
        # Flatten
        referencedParamNames = [item for sublist in referencableParamGroupList for item in sublist]
        
        #### Check accessions outside referenceableParamGroupRef
        localParamNames = [x.get('name') for x in config.findall(".//{*}cvParam")]
        
        allParamNames = referencedParamNames + localParamNames
        
        #### Check for model/vendor
        possible_instrument_models = [x for x in allParamNames if x in model_names]
        possible_instrument_vendors = [x for x in allParamNames if x in instrument_vendor_names]
        
        # It appears that some software uses the old mzXML tagging in userParams
        possible_instrument_models  += [x.get('value') for x in config.findall(".//{*}userParam[@name='msModel']")]
        possible_instrument_vendors += [x.get('value') for x in config.findall(".//{*}userParam[@name='msManufacturer']")]

        if len(possible_instrument_models) >= 1:
            instrumentConfigurationDict[key]['model'] = [possible_instrument_models[0]]
        if len(possible_instrument_models) > 1:
            print(f"Multiple instrument models not supported, taking the first for file {input_file}")
        
        if len(possible_instrument_vendors) >= 1:
            instrumentConfigurationDict[key]['vendor'] = [possible_instrument_vendors[0]]
        if len(possible_instrument_vendors) > 1:
            print(f"Multiple instrument vendors not supported, taking the first for file {input_file}")
        ##########################################################
        componentList = config.find(".//{*}componentList")
        if componentList is not None:
            source = [x.get('name', '') for x in componentList.findall(".//{*}source//{*}cvParam")]
            analyzer = [x.get('name', '') for x in componentList.findall(".//{*}analyzer//{*}cvParam")]
            detector = [x.get('name', '') for x in componentList.findall(".//{*}detector//{*}cvParam")]
            
            # It appears that some software uses the old mzXML tagging in userParams
            source   += [x.get('value', '') for x in componentList.findall(".//{*}source//{*}userParam[@name='msIonisation']")]
            analyzer += [x.get('value', '') for x in componentList.findall(".//{*}analyzer//{*}userParam[@name='msMassAnalyzer']")]
            detector += [x.get('value', '') for x in componentList.findall(".//{*}detector//{*}userParam[@name='msDetector']")]

        
            if len(source) > 0 :
                instrumentConfigurationDict[key]['source'] = source
            if len(analyzer) > 0 :
                instrumentConfigurationDict[key]['analyzer'] = analyzer
            if len(detector) > 0 :
                instrumentConfigurationDict[key]['detector'] = detector

            
    # For each field, we will seperate the instrument configurations by '|' and the components of each configuration by ;
    # This is probably best stored as a JSON, rather than a CSV, but this has downstream impact
    # Join together each field within configuration by ';'
    for key in instrumentConfigurationDict.keys():
        for field in instrumentConfigurationDict[key].keys():
            print(instrumentConfigurationDict[key][field])
            if instrumentConfigurationDict[key][field] is not None:
                instrumentConfigurationDict[key][field] = [x for x in instrumentConfigurationDict[key][field] if x is not None]
                instrumentConfigurationDict[key][field] = ';'.join(instrumentConfigurationDict[key][field])
            else: 
                 instrumentConfigurationDict[key][field] = ''
    # Join together each field by '|'. Note that the numner of '|' will be the same for each field, regardless of whether there is data
    output_dictionary['Ion_Source'] = '|'.join([instrumentConfigurationDict[x].get('source', '') for x in instrumentConfigurationDict.keys()])
    output_dictionary['Mass_Analyzer'] = '|'.join([instrumentConfigurationDict[x].get('analyzer', '') for x in instrumentConfigurationDict.keys()])
    output_dictionary['Mass_Detector'] = '|'.join([instrumentConfigurationDict[x].get('detector', '') for x in instrumentConfigurationDict.keys()])
    output_dictionary['Model'] = '|'.join([instrumentConfigurationDict[x].get('model', '') for x in instrumentConfigurationDict.keys()])
    output_dictionary['Vendor'] = '|'.join([instrumentConfigurationDict[x].get('vendor', '') for x in instrumentConfigurationDict.keys()])
    
    ############## Parse Spectrum Information ##################
    all_spectra = lxml_tree.findall("//{*}spectrum")
    all_ms_level_elements = [x.findall(".//{*}cvParam[@name='ms level']") for x in all_spectra]
    # Flatten
    all_ms_level_elements = [item for sublist in all_ms_level_elements for item in sublist]
    # Remove Nones
    all_ms_level_elements = [x for x in all_ms_level_elements if x is not None]
    all_ms_levels = [x.get('value') for x in all_ms_level_elements] # List of ms levels as type str
    # Get counts up to MS 9 and 10+
    ms_level_counts = [all_ms_levels.count(str(x)) for x in range(1,10)] + [sum([int(x) > 9 for x in all_ms_levels])]
    for mslevel in range(1,10):
        output_dictionary[f"MS{mslevel}s"] = ms_level_counts[mslevel-1]
    output_dictionary["MS10+"] = ms_level_counts[-1]

    with open(output_filename, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=HEADERS, delimiter='\t')
        writer.writeheader()
        writer.writerow(output_dictionary)

def process_file(command_line_args):
    """Processes an mzML, mzXML, or MGF file and summarizes all scans in a CSV file.

    Args:
        command_line_args (Namespace): Command line arguments form main
            should contain input_spectrum_file, original_path, result_file, and obo_file (only for mzML)
            
    """
    input_file = command_line_args.input_spectrum_file
    original_path = command_line_args.original_path
    output_filename = command_line_args.result_file
    
    if input_file.endswith(".mzML"):
        ontology_file = command_line_args.ontology_file
        process_mzML(input_file, original_path, output_filename, ontology_file)
    elif input_file.endswith(".mzXML"):
        process_mzXML(input_file, original_path, output_filename)
    elif input_file.endswith(".mgf"):
        process_MGF(input_file, original_path, output_filename)
    else:
        print(f"Unknown filetype for file {original_path}")
        sys.exit()  # Exit cleanly so we don't show errors in nextflow for this


def main():
    parser = argparse.ArgumentParser(description='Running library search parallel')
    parser.add_argument('--input_spectrum_file', help='input_spectrum_file')
    parser.add_argument('--original_path', help='original_path')
    parser.add_argument('--result_file', help='result_file')
    parser.add_argument('--ontology_file', default=None, help='obo_file')
    parser.add_argument('--error_name', default=None, help='File path for error file')
    args = parser.parse_args()

    try:
        process_file(args)
    except Exception as any_exception:
        if args.error_name is None:
            raise any_exception
        with open(args.error_name, 'w') as f:
            f.write("Error processing file: {args.input}")
            f.write(str(any_exception))
            
if __name__ == "__main__":
    main()