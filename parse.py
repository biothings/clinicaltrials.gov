import json
import os
import glob

from biothings.utils.dataload import dict_sweep


def load_data(data_folder):
    for infile in glob.glob(os.path.join(data_folder,"*.json")):
        with open(infile, 'rb') as file:
            doc = json.load(file)

        studies = doc["studies"]

        for study in studies:
            yield dict_sweep(study, ['','null', 'N/A', None, [], {}])


def load_data_file(input_file):
    with open(input_file) as f:
        doc = json.load(f)

        studies = doc["studies"]
        for study in studies:
            study["_id"] = study['protocolSection']['identificationModule']['nctId']
            yield dict_sweep(study, ['','null', 'N/A', None, [], {}])