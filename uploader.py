import os
import glob
import json

import biothings, config

biothings.config_for_app(config)

import biothings.hub.dataload.uploader
# from .parse import load_data

from biothings.utils.dataload import dict_sweep


class ClinicalTrialsGovUploader(biothings.hub.dataload.uploader.BaseSourceUploader):
    name = "clinicaltrials_gov"
    __metadata__ = {
        "src_meta": {
            "url": "https://www.clinicaltrials.gov/",
            "license_url": "https://www.clinicaltrials.gov/about-site/terms-conditions",
        }
    }

    idconverter = None
    storage_class = biothings.hub.dataload.storage.IgnoreDuplicatedStorage

    def load_data(self, data_folder):
        self.logger.info("Loading data from directory: '%s'" % data_folder)
        for infile in glob.glob(os.path.join(data_folder,"*.json")):
            self.logger.info("Opening json from infile: %s" % infile)
            doc = json.load(open(infile))

            self.logger.info("Outputting json document: %s" % doc)

            studies = doc["studies"]

            for study in studies:
                yield dict_sweep(study, ['','null', 'N/A', None, [], {}])
        # return load_data(data_folder)
