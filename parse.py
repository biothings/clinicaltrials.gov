import json
import os
import glob

from biothings.utils.dataload import dict_sweep


def load_data(data_folder):
    for infile in glob.glob(os.path.join(data_folder,"*.json")):
        doc = json.load(open(infile))

        studies = doc["studies"]

        for study in studies:
            yield dict_sweep(study, ['','null', 'N/A', None, [], {}])