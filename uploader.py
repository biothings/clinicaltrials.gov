import os
import json

import biothings, config

biothings.config_for_app(config)

import biothings.hub.dataload.uploader
from .parse import load_data


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
        return load_data(data_folder)
