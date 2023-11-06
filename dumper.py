import os
import time
import requests
from math import ceil
import json

from config import DATA_ARCHIVE_ROOT

from biothings.hub.dataload.dumper import HTTPDumper

class ClinicalTrialsGovDumper(HTTPDumper):
    SRC_NAME = "clinicaltrials_gov"
    SRC_ROOT_FOLDER = os.path.join(DATA_ARCHIVE_ROOT, SRC_NAME)
    BASE_URL = "https://clinicaltrials.gov/api/v2/studies"
    SCHEDULE = "0 6 * * *"
    PAGE_SIZE = 1000  # Number of studies to request per page
    REQUEST_DELAY = 1 / 3  # Request delay to stay within API rate limits

    def get_total_studies(self):
        size = requests.get("https://clinicaltrials.gov/api/v2/stats/size")
        total_studies = size.json()["totalStudies"]
        return total_studies

    def create_todump_list(self, force=False):
        resp = requests.get("https://clinicaltrials.gov/api/v2/version").json()
        self.release = resp["dataTimestamp"].split("T")[0]

        if force or not self.current_release or int(self.release) > int(self.current_release):
            self.to_dump.append({
                "remote": self.BASE_URL,
                "local": os.path.join(self.new_data_folder, "clinicaltrials_gov.json")
            })

    def download(self, remoteurl, localfile, headers={}):
        self.prepare_local_folders(localfile)

        total_studies = self.get_total_studies()

        aggregated_studies = []
        next_page = None

        for page in range(ceil(total_studies / self.PAGE_SIZE)):
            payload = {
                "format": "json",
                "pageSize": str(self.PAGE_SIZE),
                "pageToken": str(next_page) if next_page else None
            }
            data = requests.get(remoteurl, params=payload, headers=headers)
            studies = data.json()

            aggregated_studies.extend(studies["studies"])

            if "nextPageToken" not in studies:
                break

            next_page = studies["nextPageToken"]

            time.sleep(self.REQUEST_DELAY)

        with open(localfile, "w") as fout:
            json.dump(aggregated_studies, fout)

        return None  # Return None to indicate success

    def remote_is_better(self, remotefile, localfile):
        """
        Determine if remote is better

        Override if necessary.
        """
        # Always redownload the data since the release is the total number of studies
        return True
