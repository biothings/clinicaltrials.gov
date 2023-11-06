import os
import json

import biothings, config

biothings.config_for_app(config)

import biothings.hub.dataload.uploader


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
        self.logger.info("Load data from directory: '%s'" % data_folder)
        infile = os.path.join(data_folder, "clinical_trials.json")  # Corrected file name
        assert os.path.exists(infile)
        with open(infile, "r") as f:
            data = json.load(f)  # Read the entire JSON file as a list
            for entry in data:
                yield entry