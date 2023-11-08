import os
import time
import requests
from math import ceil
import json

from config import DATA_ARCHIVE_ROOT

from biothings.hub.dataload.dumper import HTTPDumper

# try:
#     from biothings import config
#     logger = config.logger
# except ImportError:
#     import logging
#     logger = logging.getLogger(__name__)


class ClinicalTrialsGovDumper(HTTPDumper):
    SRC_NAME = "clinicaltrials_gov"
    SRC_ROOT_FOLDER = os.path.join(DATA_ARCHIVE_ROOT, SRC_NAME)
    API_PAGE = "https://clinicaltrials.gov/api/v2/studies"
    PAGE_SIZE = 1000  # Number of studies to request per page

    SLEEP_BETWEEN_DOWNLOAD = 0.33

    def get_total_studies(self):
        size = requests.get("https://clinicaltrials.gov/api/v2/stats/size")
        total_studies = size.json()["totalStudies"]
        return total_studies

    def create_todump_list(self, force=False):
        resp = requests.get("https://clinicaltrials.gov/api/v2/version").json()
        self.release = resp["dataTimestamp"].split("T")[0]

        self.logger.info("Downloading all available trial data")
        total_pages = ceil(self.get_total_studies() / 1000)

        ids = []
        pageTokens = []
        nextPage = None
        for p in range(1, total_pages + 1):
            self.logger.info(f"Handling page# {p} / {total_pages}")
            if nextPage:
                doc = self.client.get(self.API_PAGE + "?fields=NCTId&pageSize=1000&pageToken=%s" % nextPage).json()
            else:
                doc = self.client.get(self.API_PAGE + "?fields=NCTId&pageSize=1000").json()

            if 'nextPageToken' in doc:
                nextPage = doc['nextPageToken']
                pageTokens.append(nextPage)
            
            # for study in doc['studies']:
            #     ids.append(study['protocolSection']['identificationModule']['nctId'])

        self.logger.info("Now generating download URLs")
        # for id in ids:
        #     remote_file = self.API_PAGE + "/%s" % str(id)
        #     local_file = os.path.join(self.new_data_folder,"%s.json" % id)
        #     self.to_dump.append({"remote":remote_file,"local":local_file})

        # Add the first page to the download URLs
        self.to_dump.append({"remote":self.API_PAGE + "?pageSize=1000","local":os.path.join(self.new_data_folder, "firstPage.json")}) 
        for page in pageTokens:
            remote_file = self.API_PAGE + "?pageSize=1000&pageToken=%s" % str(page)
            local_file = os.path.join(self.new_data_folder,"%s.json" % page)
            self.to_dump.append({"remote":remote_file,"local":local_file}) 


    # def download(self, remoteurl, localfile, headers={}):
    #     self.prepare_local_folders(localfile)

    #     total_studies = self.get_total_studies()

    #     aggregated_studies = []
    #     next_page = None

    #     for page in range(ceil(total_studies / self.PAGE_SIZE)):
    #         logger.info(f"Handling document #{page}")
    #         payload = {
    #             "format": "json",
    #             "pageSize": str(self.PAGE_SIZE),
    #             "pageToken": str(next_page) if next_page else None
    #         }
            
    #         data = requests.get(remoteurl, params=payload, headers=headers)
                
    #         studies = data.json()

    #         aggregated_studies.extend(studies["studies"])

    #         if "nextPageToken" not in studies:
    #             break

    #         next_page = studies["nextPageToken"]

    #     with open(localfile, "w") as fout:
    #         json.dump(aggregated_studies, fout)

    #     return None  # Return None to indicate success
