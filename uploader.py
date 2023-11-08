import os
import glob
import json

import biothings, config

biothings.config_for_app(config)

import biothings.hub.dataload.uploader
from .parse import load_data, load_data_file

from biothings.utils.dataload import dict_sweep


class ClinicalTrialsGovUploader(biothings.hub.dataload.uploader.ParallelizedSourceUploader):
    name = "clinicaltrials_gov"
    __metadata__ = {
        "src_meta": {
            "url": "https://www.clinicaltrials.gov/",
            "license_url": "https://www.clinicaltrials.gov/about-site/terms-conditions",
        }
    }

    def jobs(self):
        files = glob.glob(os.path.join(self.data_folder, "*.json"))
        return [(f, ) for f in files]

    def load_data(self, input_file):
        self.logger.info("Processing data from %s" % input_file)
        return load_data_file(input_file)

    @classmethod
    def get_mapping(klass):
        mapping = {
            "protocolSection": {
                "properties": {
                    "identificationModule": {
                        "properties": {
                            "nctId": {
                                "normalizer": "keyword_lowercase_normalizer",
                                "type": "keyword"
                            },
                            "orgStudyIdInfo": {
                                "properties": {
                                    "id": {
                                        "type": "text"
                                    },
                                    "type": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "link": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    }
                                }
                            },
                            "organization": {
                                "properties": {
                                    "fullName": {
                                        "type": "text"
                                    },
                                    "class": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    }
                                }
                            },
                            "secondaryIdInfos": {
                                "properties": {
                                    "type": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "link": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "id": {
                                        "type": "text"
                                    },
                                    "domain": {
                                        "type": "text"
                                    }
                                }
                            },
                            "nctIdAliases": {
                                "normalizer": "keyword_lowercase_normalizer",
                                "type": "keyword"
                            },
                            "briefTitle": {
                                "type": "text"
                            },
                            "officialTitle": {
                                "type": "text"
                            },
                            "acronym": {
                                "type": "text"
                            }
                        }
                    },
                    "statusModule": {
                        "properties": {
                            "statusVerifiedDate": {
                                "normalizer": "keyword_lowercase_normalizer",
                                "type": "keyword"
                            },
                            "overallStatus": {
                                "normalizer": "keyword_lowercase_normalizer",
                                "type": "keyword"
                            },
                            "expandedAccessInfo": {
                                "properties": {
                                    "hasExpandedAccess": {
                                        "type": "boolean"
                                    },
                                    "nctId": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "statusForNctId": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    }
                                }
                            },
                            "startDateStruct": {
                                "properties": {
                                    "date": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "type": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    }
                                }
                            },
                            "completionDateStruct": {
                                "properties": {
                                    "date": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "type": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    }
                                }
                            },
                            "studyFirstSubmitDate": {
                                "normalizer": "keyword_lowercase_normalizer",
                                "type": "keyword"
                            },
                            "studyFirstSubmitQcDate": {
                                "normalizer": "keyword_lowercase_normalizer",
                                "type": "keyword"
                            },
                            "studyFirstPostDateStruct": {
                                "properties": {
                                    "date": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "type": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    }
                                }
                            },
                            "lastUpdateSubmitDate": {
                                "normalizer": "keyword_lowercase_normalizer",
                                "type": "keyword"
                            },
                            "lastUpdatePostDateStruct": {
                                "properties": {
                                    "date": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "type": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    }
                                }
                            },
                            "primaryCompletionDateStruct": {
                                "properties": {
                                    "date": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "type": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    }
                                }
                            },
                            "dispFirstSubmitDate": {
                                "normalizer": "keyword_lowercase_normalizer",
                                "type": "keyword"
                            },
                            "dispFirstSubmitQcDate": {
                                "normalizer": "keyword_lowercase_normalizer",
                                "type": "keyword"
                            },
                            "dispFirstPostDateStruct": {
                                "properties": {
                                    "date": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "type": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    }
                                }
                            },
                            "lastKnownStatus": {
                                "normalizer": "keyword_lowercase_normalizer",
                                "type": "keyword"
                            },
                            "resultsFirstSubmitDate": {
                                "normalizer": "keyword_lowercase_normalizer",
                                "type": "keyword"
                            },
                            "resultsFirstSubmitQcDate": {
                                "normalizer": "keyword_lowercase_normalizer",
                                "type": "keyword"
                            },
                            "resultsFirstPostDateStruct": {
                                "properties": {
                                    "date": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "type": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    }
                                }
                            },
                            "delayedPosting": {
                                "type": "boolean"
                            },
                            "whyStopped": {
                                "type": "text"
                            }
                        }
                    },
                    "sponsorCollaboratorsModule": {
                        "properties": {
                            "leadSponsor": {
                                "properties": {
                                    "name": {
                                        "type": "text"
                                    },
                                    "class": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    }
                                }
                            },
                            "responsibleParty": {
                                "properties": {
                                    "type": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "investigatorFullName": {
                                        "type": "text"
                                    },
                                    "investigatorTitle": {
                                        "type": "text"
                                    },
                                    "investigatorAffiliation": {
                                        "type": "text"
                                    },
                                    "oldNameTitle": {
                                        "type": "text"
                                    },
                                    "oldOrganization": {
                                        "type": "text"
                                    }
                                }
                            },
                            "collaborators": {
                                "properties": {
                                    "class": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "name": {
                                        "type": "text"
                                    }
                                }
                            }
                        }
                    },
                    "descriptionModule": {
                        "properties": {
                            "briefSummary": {
                                "type": "text"
                            },
                            "detailedDescription": {
                                "type": "text"
                            }
                        }
                    },
                    "conditionsModule": {
                        "properties": {
                            "conditions": {
                                "type": "text"
                            },
                            "keywords": {
                                "type": "text"
                            }
                        }
                    },
                    "designModule": {
                        "properties": {
                            "studyType": {
                                "normalizer": "keyword_lowercase_normalizer",
                                "type": "keyword"
                            },
                            "phases": {
                                "normalizer": "keyword_lowercase_normalizer",
                                "type": "keyword"
                            },
                            "designInfo": {
                                "properties": {
                                    "allocation": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "primaryPurpose": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "maskingInfo": {
                                        "properties": {
                                            "masking": {
                                                "normalizer": "keyword_lowercase_normalizer",
                                                "type": "keyword"
                                            },
                                            "whoMasked": {
                                                "normalizer": "keyword_lowercase_normalizer",
                                                "type": "keyword"
                                            },
                                            "maskingDescription": {
                                                "type": "text"
                                            }
                                        }
                                    },
                                    "interventionModel": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "observationalModel": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "timePerspective": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "interventionModelDescription": {
                                        "type": "text"
                                    }
                                }
                            },
                            "enrollmentInfo": {
                                "properties": {
                                    "count": {
                                        "type": "integer"
                                    },
                                    "type": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    }
                                }
                            },
                            "bioSpec": {
                                "properties": {
                                    "retention": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "description": {
                                        "type": "text"
                                    }
                                }
                            },
                            "patientRegistry": {
                                "type": "boolean"
                            },
                            "nPtrsToThisExpAccNctId": {
                                "type": "integer"
                            },
                            "expandedAccessTypes": {
                                "properties": {
                                    "intermediate": {
                                        "type": "boolean"
                                    },
                                    "individual": {
                                        "type": "boolean"
                                    },
                                    "treatment": {
                                        "type": "boolean"
                                    }
                                }
                            },
                            "targetDuration": {
                                "type": "text"
                            }
                        }
                    },
                    "armsInterventionsModule": {
                        "properties": {
                            "interventions": {
                                "properties": {
                                    "type": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "armGroupLabels": {
                                        "type": "text"
                                    },
                                    "otherNames": {
                                        "type": "text"
                                    },
                                    "name": {
                                        "type": "text"
                                    },
                                    "description": {
                                        "type": "text"
                                    }
                                }
                            },
                            "armGroups": {
                                "properties": {
                                    "type": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "interventionNames": {
                                        "type": "text"
                                    },
                                    "label": {
                                        "type": "text"
                                    },
                                    "description": {
                                        "type": "text"
                                    }
                                }
                            }
                        }
                    },
                    "eligibilityModule": {
                        "properties": {
                            "healthyVolunteers": {
                                "type": "boolean"
                            },
                            "sex": {
                                "normalizer": "keyword_lowercase_normalizer",
                                "type": "keyword"
                            },
                            "stdAges": {
                                "normalizer": "keyword_lowercase_normalizer",
                                "type": "keyword"
                            },
                            "samplingMethod": {
                                "normalizer": "keyword_lowercase_normalizer",
                                "type": "keyword"
                            },
                            "genderBased": {
                                "type": "boolean"
                            },
                            "eligibilityCriteria": {
                                "type": "text"
                            },
                            "minimumAge": {
                                "type": "text"
                            },
                            "maximumAge": {
                                "type": "text"
                            },
                            "studyPopulation": {
                                "type": "text"
                            },
                            "genderDescription": {
                                "type": "text"
                            }
                        }
                    },
                    "referencesModule": {
                        "properties": {
                            "references": {
                                "properties": {
                                    "pmid": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "type": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "retractions": {
                                        "properties": {
                                            "pmid": {
                                                "normalizer": "keyword_lowercase_normalizer",
                                                "type": "keyword"
                                            },
                                            "source": {
                                                "type": "text"
                                            }
                                        }
                                    },
                                    "citation": {
                                        "type": "text"
                                    }
                                }
                            },
                            "seeAlsoLinks": {
                                "properties": {
                                    "url": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "label": {
                                        "type": "text"
                                    }
                                }
                            },
                            "availIpds": {
                                "properties": {
                                    "url": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "id": {
                                        "type": "text"
                                    },
                                    "type": {
                                        "type": "text"
                                    },
                                    "comment": {
                                        "type": "text"
                                    }
                                }
                            }
                        }
                    },
                    "outcomesModule": {
                        "properties": {
                            "primaryOutcomes": {
                                "properties": {
                                    "measure": {
                                        "type": "text"
                                    },
                                    "description": {
                                        "type": "text"
                                    },
                                    "timeFrame": {
                                        "type": "text"
                                    }
                                }
                            },
                            "secondaryOutcomes": {
                                "properties": {
                                    "measure": {
                                        "type": "text"
                                    },
                                    "description": {
                                        "type": "text"
                                    },
                                    "timeFrame": {
                                        "type": "text"
                                    }
                                }
                            },
                            "otherOutcomes": {
                                "properties": {
                                    "measure": {
                                        "type": "text"
                                    },
                                    "description": {
                                        "type": "text"
                                    },
                                    "timeFrame": {
                                        "type": "text"
                                    }
                                }
                            }
                        }
                    },
                    "contactsLocationsModule": {
                        "properties": {
                            "overallOfficials": {
                                "properties": {
                                    "role": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "name": {
                                        "type": "text"
                                    },
                                    "affiliation": {
                                        "type": "text"
                                    }
                                }
                            },
                            "locations": {
                                "properties": {
                                    "geoPoint": {
                                        "properties": {
                                            "lat": {
                                                "type": "float"
                                            },
                                            "lon": {
                                                "type": "float"
                                            }
                                        }
                                    },
                                    "status": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "contacts": {
                                        "properties": {
                                            "role": {
                                                "normalizer": "keyword_lowercase_normalizer",
                                                "type": "keyword"
                                            },
                                            "email": {
                                                "type": "text"
                                            },
                                            "phoneExt": {
                                                "type": "text"
                                            },
                                            "phone": {
                                                "type": "text"
                                            },
                                            "name": {
                                                "type": "text"
                                            }
                                        }
                                    },
                                    "zip": {
                                        "type": "text"
                                    },
                                    "city": {
                                        "type": "text"
                                    },
                                    "state": {
                                        "type": "text"
                                    },
                                    "country": {
                                        "type": "text"
                                    },
                                    "facility": {
                                        "type": "text"
                                    }
                                }
                            },
                            "centralContacts": {
                                "properties": {
                                    "role": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "phoneExt": {
                                        "type": "text"
                                    },
                                    "email": {
                                        "type": "text"
                                    },
                                    "phone": {
                                        "type": "text"
                                    },
                                    "name": {
                                        "type": "text"
                                    }
                                }
                            }
                        }
                    },
                    "oversightModule": {
                        "properties": {
                            "oversightHasDmc": {
                                "type": "boolean"
                            },
                            "isFdaRegulatedDrug": {
                                "type": "boolean"
                            },
                            "isFdaRegulatedDevice": {
                                "type": "boolean"
                            },
                            "isUsExport": {
                                "type": "boolean"
                            },
                            "isUnapprovedDevice": {
                                "type": "boolean"
                            },
                            "isPpsd": {
                                "type": "boolean"
                            },
                            "fdaaa801Violation": {
                                "type": "boolean"
                            }
                        }
                    },
                    "ipdSharingStatementModule": {
                        "properties": {
                            "ipdSharing": {
                                "normalizer": "keyword_lowercase_normalizer",
                                "type": "keyword"
                            },
                            "infoTypes": {
                                "normalizer": "keyword_lowercase_normalizer",
                                "type": "keyword"
                            },
                            "url": {
                                "type": "text"
                            },
                            "description": {
                                "type": "text"
                            },
                            "timeFrame": {
                                "type": "text"
                            },
                            "accessCriteria": {
                                "type": "text"
                            }
                        }
                    }
                }
            },
            "derivedSection": {
                "properties": {
                    "miscInfoModule": {
                        "properties": {
                            "versionHolder": {
                                "normalizer": "keyword_lowercase_normalizer",
                                "type": "keyword"
                            },
                            "modelPredictions": {
                                "properties": {
                                    "bmiLimits": {
                                        "properties": {
                                            "minBmi": {
                                                "type": "float"
                                            },
                                            "maxBmi": {
                                                "type": "float"
                                            }
                                        }
                                    }
                                }
                            },
                            "removedCountries": {
                                "type": "text"
                            },
                            "submissionTracking": {
                                "properties": {
                                    "estimatedResultsFirstSubmitDate": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "submissionInfos": {
                                        "properties": {
                                            "releaseDate": {
                                                "normalizer": "keyword_lowercase_normalizer",
                                                "type": "keyword"
                                            },
                                            "resetDate": {
                                                "normalizer": "keyword_lowercase_normalizer",
                                                "type": "keyword"
                                            },
                                            "unreleaseDateUnknown": {
                                                "type": "boolean"
                                            },
                                            "unreleaseDate": {
                                                "normalizer": "keyword_lowercase_normalizer",
                                                "type": "keyword"
                                            },
                                            "mcpReleaseN": {
                                                "type": "integer"
                                            }
                                        }
                                    },
                                    "firstMcpInfo": {
                                        "properties": {
                                            "postDateStruct": {
                                                "properties": {
                                                    "date": {
                                                        "normalizer": "keyword_lowercase_normalizer",
                                                        "type": "keyword"
                                                    },
                                                    "type": {
                                                        "normalizer": "keyword_lowercase_normalizer",
                                                        "type": "keyword"
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    },
                    "conditionBrowseModule": {
                        "properties": {
                            "meshes": {
                                "properties": {
                                    "id": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "term": {
                                        "type": "text"
                                    }
                                }
                            },
                            "ancestors": {
                                "properties": {
                                    "id": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "term": {
                                        "type": "text"
                                    }
                                }
                            },
                            "browseLeaves": {
                                "properties": {
                                    "id": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "relevance": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "asFound": {
                                        "type": "text"
                                    },
                                    "name": {
                                        "type": "text"
                                    }
                                }
                            },
                            "browseBranches": {
                                "properties": {
                                    "abbrev": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "name": {
                                        "type": "text"
                                    }
                                }
                            }
                        }
                    },
                    "interventionBrowseModule": {
                        "properties": {
                            "meshes": {
                                "properties": {
                                    "id": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "term": {
                                        "type": "text"
                                    }
                                }
                            },
                            "ancestors": {
                                "properties": {
                                    "id": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "term": {
                                        "type": "text"
                                    }
                                }
                            },
                            "browseLeaves": {
                                "properties": {
                                    "id": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "relevance": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "asFound": {
                                        "type": "text"
                                    },
                                    "name": {
                                        "type": "text"
                                    }
                                }
                            },
                            "browseBranches": {
                                "properties": {
                                    "abbrev": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "name": {
                                        "type": "text"
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "hasResults": {
                "type": "boolean"
            },
            "resultsSection": {
                "properties": {
                    "participantFlowModule": {
                        "properties": {
                            "groups": {
                                "properties": {
                                    "id": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "title": {
                                        "type": "text"
                                    },
                                    "description": {
                                        "type": "text"
                                    }
                                }
                            },
                            "periods": {
                                "properties": {
                                    "milestones": {
                                        "properties": {
                                            "achievements": {
                                                "properties": {
                                                    "groupId": {
                                                        "normalizer": "keyword_lowercase_normalizer",
                                                        "type": "keyword"
                                                    },
                                                    "numSubjects": {
                                                        "normalizer": "keyword_lowercase_normalizer",
                                                        "type": "keyword"
                                                    },
                                                    "numUnits": {
                                                        "normalizer": "keyword_lowercase_normalizer",
                                                        "type": "keyword"
                                                    },
                                                    "comment": {
                                                        "type": "text"
                                                    }
                                                }
                                            },
                                            "comment": {
                                                "type": "text"
                                            },
                                            "type": {
                                                "type": "text"
                                            }
                                        }
                                    },
                                    "dropWithdraws": {
                                        "properties": {
                                            "reasons": {
                                                "properties": {
                                                    "groupId": {
                                                        "normalizer": "keyword_lowercase_normalizer",
                                                        "type": "keyword"
                                                    },
                                                    "numSubjects": {
                                                        "normalizer": "keyword_lowercase_normalizer",
                                                        "type": "keyword"
                                                    }
                                                }
                                            },
                                            "type": {
                                                "type": "text"
                                            }
                                        }
                                    },
                                    "title": {
                                        "type": "text"
                                    }
                                }
                            },
                            "recruitmentDetails": {
                                "type": "text"
                            },
                            "preAssignmentDetails": {
                                "type": "text"
                            },
                            "typeUnitsAnalyzed": {
                                "type": "text"
                            }
                        }
                    },
                    "baselineCharacteristicsModule": {
                        "properties": {
                            "groups": {
                                "properties": {
                                    "id": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "title": {
                                        "type": "text"
                                    },
                                    "description": {
                                        "type": "text"
                                    }
                                }
                            },
                            "denoms": {
                                "properties": {
                                    "counts": {
                                        "properties": {
                                            "groupId": {
                                                "normalizer": "keyword_lowercase_normalizer",
                                                "type": "keyword"
                                            },
                                            "value": {
                                                "normalizer": "keyword_lowercase_normalizer",
                                                "type": "keyword"
                                            }
                                        }
                                    },
                                    "units": {
                                        "type": "text"
                                    }
                                }
                            },
                            "measures": {
                                "properties": {
                                    "paramType": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "classes": {
                                        "properties": {
                                            "categories": {
                                                "properties": {
                                                    "measurements": {
                                                        "properties": {
                                                            "groupId": {
                                                                "normalizer": "keyword_lowercase_normalizer",
                                                                "type": "keyword"
                                                            },
                                                            "value": {
                                                                "normalizer": "keyword_lowercase_normalizer",
                                                                "type": "keyword"
                                                            },
                                                            "lowerLimit": {
                                                                "normalizer": "keyword_lowercase_normalizer",
                                                                "type": "keyword"
                                                            },
                                                            "upperLimit": {
                                                                "normalizer": "keyword_lowercase_normalizer",
                                                                "type": "keyword"
                                                            },
                                                            "spread": {
                                                                "type": "text"
                                                            },
                                                            "comment": {
                                                                "type": "text"
                                                            }
                                                        }
                                                    },
                                                    "title": {
                                                        "type": "text"
                                                    }
                                                }
                                            },
                                            "denoms": {
                                                "properties": {
                                                    "counts": {
                                                        "properties": {
                                                            "groupId": {
                                                                "normalizer": "keyword_lowercase_normalizer",
                                                                "type": "keyword"
                                                            },
                                                            "value": {
                                                                "normalizer": "keyword_lowercase_normalizer",
                                                                "type": "keyword"
                                                            }
                                                        }
                                                    },
                                                    "units": {
                                                        "type": "text"
                                                    }
                                                }
                                            },
                                            "title": {
                                                "type": "text"
                                            }
                                        }
                                    },
                                    "dispersionType": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "calculatePct": {
                                        "type": "boolean"
                                    },
                                    "denoms": {
                                        "properties": {
                                            "counts": {
                                                "properties": {
                                                    "groupId": {
                                                        "normalizer": "keyword_lowercase_normalizer",
                                                        "type": "keyword"
                                                    },
                                                    "value": {
                                                        "normalizer": "keyword_lowercase_normalizer",
                                                        "type": "keyword"
                                                    }
                                                }
                                            },
                                            "units": {
                                                "type": "text"
                                            }
                                        }
                                    },
                                    "denomUnitsSelected": {
                                        "type": "text"
                                    },
                                    "unitOfMeasure": {
                                        "type": "text"
                                    },
                                    "populationDescription": {
                                        "type": "text"
                                    },
                                    "title": {
                                        "type": "text"
                                    },
                                    "description": {
                                        "type": "text"
                                    }
                                }
                            },
                            "populationDescription": {
                                "type": "text"
                            },
                            "typeUnitsAnalyzed": {
                                "type": "text"
                            }
                        }
                    },
                    "outcomeMeasuresModule": {
                        "properties": {
                            "outcomeMeasures": {
                                "properties": {
                                    "type": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "reportingStatus": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "paramType": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "groups": {
                                        "properties": {
                                            "id": {
                                                "normalizer": "keyword_lowercase_normalizer",
                                                "type": "keyword"
                                            },
                                            "title": {
                                                "type": "text"
                                            },
                                            "description": {
                                                "type": "text"
                                            }
                                        }
                                    },
                                    "denoms": {
                                        "properties": {
                                            "counts": {
                                                "properties": {
                                                    "groupId": {
                                                        "normalizer": "keyword_lowercase_normalizer",
                                                        "type": "keyword"
                                                    },
                                                    "value": {
                                                        "normalizer": "keyword_lowercase_normalizer",
                                                        "type": "keyword"
                                                    }
                                                }
                                            },
                                            "units": {
                                                "type": "text"
                                            }
                                        }
                                    },
                                    "classes": {
                                        "properties": {
                                            "categories": {
                                                "properties": {
                                                    "measurements": {
                                                        "properties": {
                                                            "groupId": {
                                                                "normalizer": "keyword_lowercase_normalizer",
                                                                "type": "keyword"
                                                            },
                                                            "value": {
                                                                "normalizer": "keyword_lowercase_normalizer",
                                                                "type": "keyword"
                                                            },
                                                            "lowerLimit": {
                                                                "normalizer": "keyword_lowercase_normalizer",
                                                                "type": "keyword"
                                                            },
                                                            "upperLimit": {
                                                                "type": "text"
                                                            },
                                                            "spread": {
                                                                "type": "text"
                                                            },
                                                            "comment": {
                                                                "type": "text"
                                                            }
                                                        }
                                                    },
                                                    "title": {
                                                        "type": "text"
                                                    }
                                                }
                                            },
                                            "denoms": {
                                                "properties": {
                                                    "counts": {
                                                        "properties": {
                                                            "groupId": {
                                                                "normalizer": "keyword_lowercase_normalizer",
                                                                "type": "keyword"
                                                            },
                                                            "value": {
                                                                "normalizer": "keyword_lowercase_normalizer",
                                                                "type": "keyword"
                                                            }
                                                        }
                                                    },
                                                    "units": {
                                                        "type": "text"
                                                    }
                                                }
                                            },
                                            "title": {
                                                "type": "text"
                                            }
                                        }
                                    },
                                    "analyses": {
                                        "properties": {
                                            "groupIds": {
                                                "normalizer": "keyword_lowercase_normalizer",
                                                "type": "keyword"
                                            },
                                            "nonInferiorityType": {
                                                "normalizer": "keyword_lowercase_normalizer",
                                                "type": "keyword"
                                            },
                                            "ciPctValue": {
                                                "normalizer": "keyword_lowercase_normalizer",
                                                "type": "keyword"
                                            },
                                            "ciNumSides": {
                                                "normalizer": "keyword_lowercase_normalizer",
                                                "type": "keyword"
                                            },
                                            "testedNonInferiority": {
                                                "type": "boolean"
                                            },
                                            "dispersionType": {
                                                "normalizer": "keyword_lowercase_normalizer",
                                                "type": "keyword"
                                            },
                                            "dispersionValue": {
                                                "normalizer": "keyword_lowercase_normalizer",
                                                "type": "keyword"
                                            },
                                            "ciLowerLimit": {
                                                "type": "text"
                                            },
                                            "ciUpperLimit": {
                                                "type": "text"
                                            },
                                            "paramValue": {
                                                "type": "text"
                                            },
                                            "otherAnalysisDescription": {
                                                "type": "text"
                                            },
                                            "ciUpperLimitComment": {
                                                "type": "text"
                                            },
                                            "pValue": {
                                                "type": "text"
                                            },
                                            "nonInferiorityComment": {
                                                "type": "text"
                                            },
                                            "pValueComment": {
                                                "type": "text"
                                            },
                                            "statisticalComment": {
                                                "type": "text"
                                            },
                                            "estimateComment": {
                                                "type": "text"
                                            },
                                            "statisticalMethod": {
                                                "type": "text"
                                            },
                                            "groupDescription": {
                                                "type": "text"
                                            },
                                            "paramType": {
                                                "type": "text"
                                            }
                                        }
                                    },
                                    "anticipatedPostingDate": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "calculatePct": {
                                        "type": "boolean"
                                    },
                                    "typeUnitsAnalyzed": {
                                        "type": "text"
                                    },
                                    "denomUnitsSelected": {
                                        "type": "text"
                                    },
                                    "dispersionType": {
                                        "type": "text"
                                    },
                                    "title": {
                                        "type": "text"
                                    },
                                    "description": {
                                        "type": "text"
                                    },
                                    "populationDescription": {
                                        "type": "text"
                                    },
                                    "unitOfMeasure": {
                                        "type": "text"
                                    },
                                    "timeFrame": {
                                        "type": "text"
                                    }
                                }
                            }
                        }
                    },
                    "adverseEventsModule": {
                        "properties": {
                            "frequencyThreshold": {
                                "normalizer": "keyword_lowercase_normalizer",
                                "type": "keyword"
                            },
                            "eventGroups": {
                                "properties": {
                                    "id": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "seriousNumAffected": {
                                        "type": "integer"
                                    },
                                    "seriousNumAtRisk": {
                                        "type": "integer"
                                    },
                                    "otherNumAffected": {
                                        "type": "integer"
                                    },
                                    "otherNumAtRisk": {
                                        "type": "integer"
                                    },
                                    "deathsNumAffected": {
                                        "type": "integer"
                                    },
                                    "deathsNumAtRisk": {
                                        "type": "integer"
                                    },
                                    "title": {
                                        "type": "text"
                                    },
                                    "description": {
                                        "type": "text"
                                    }
                                }
                            },
                            "otherEvents": {
                                "properties": {
                                    "assessmentType": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "stats": {
                                        "properties": {
                                            "groupId": {
                                                "normalizer": "keyword_lowercase_normalizer",
                                                "type": "keyword"
                                            },
                                            "numEvents": {
                                                "type": "integer"
                                            },
                                            "numAffected": {
                                                "type": "integer"
                                            },
                                            "numAtRisk": {
                                                "type": "integer"
                                            }
                                        }
                                    },
                                    "notes": {
                                        "type": "text"
                                    },
                                    "term": {
                                        "type": "text"
                                    },
                                    "organSystem": {
                                        "type": "text"
                                    },
                                    "sourceVocabulary": {
                                        "type": "text"
                                    }
                                }
                            },
                            "seriousEvents": {
                                "properties": {
                                    "assessmentType": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "stats": {
                                        "properties": {
                                            "groupId": {
                                                "normalizer": "keyword_lowercase_normalizer",
                                                "type": "keyword"
                                            },
                                            "numAffected": {
                                                "type": "integer"
                                            },
                                            "numAtRisk": {
                                                "type": "integer"
                                            },
                                            "numEvents": {
                                                "type": "integer"
                                            }
                                        }
                                    },
                                    "notes": {
                                        "type": "text"
                                    },
                                    "term": {
                                        "type": "text"
                                    },
                                    "organSystem": {
                                        "type": "text"
                                    },
                                    "sourceVocabulary": {
                                        "type": "text"
                                    }
                                }
                            },
                            "timeFrame": {
                                "type": "text"
                            },
                            "description": {
                                "type": "text"
                            }
                        }
                    },
                    "moreInfoModule": {
                        "properties": {
                            "certainAgreement": {
                                "properties": {
                                    "piSponsorEmployee": {
                                        "type": "boolean"
                                    },
                                    "restrictionType": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "restrictiveAgreement": {
                                        "type": "boolean"
                                    },
                                    "otherDetails": {
                                        "type": "text"
                                    }
                                }
                            },
                            "pointOfContact": {
                                "properties": {
                                    "title": {
                                        "type": "text"
                                    },
                                    "organization": {
                                        "type": "text"
                                    },
                                    "email": {
                                        "type": "text"
                                    },
                                    "phone": {
                                        "type": "text"
                                    },
                                    "phoneExt": {
                                        "type": "text"
                                    }
                                }
                            },
                            "limitationsAndCaveats": {
                                "properties": {
                                    "description": {
                                        "type": "text"
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "annotationSection": {
                "properties": {
                    "annotationModule": {
                        "properties": {
                            "unpostedAnnotation": {
                                "properties": {
                                    "unpostedResponsibleParty": {
                                        "type": "text"
                                    },
                                    "unpostedEvents": {
                                        "properties": {
                                            "type": {
                                                "normalizer": "keyword_lowercase_normalizer",
                                                "type": "keyword"
                                            },
                                            "date": {
                                                "normalizer": "keyword_lowercase_normalizer",
                                                "type": "keyword"
                                            },
                                            "dateUnknown": {
                                                "type": "boolean"
                                            }
                                        }
                                    }
                                }
                            },
                            "violationAnnotation": {
                                "properties": {
                                    "violationEvents": {
                                        "properties": {
                                            "type": {
                                                "normalizer": "keyword_lowercase_normalizer",
                                                "type": "keyword"
                                            },
                                            "description": {
                                                "type": "text"
                                            },
                                            "creationDate": {
                                                "normalizer": "keyword_lowercase_normalizer",
                                                "type": "keyword"
                                            },
                                            "issuedDate": {
                                                "normalizer": "keyword_lowercase_normalizer",
                                                "type": "keyword"
                                            },
                                            "releaseDate": {
                                                "normalizer": "keyword_lowercase_normalizer",
                                                "type": "keyword"
                                            },
                                            "postedDate": {
                                                "normalizer": "keyword_lowercase_normalizer",
                                                "type": "keyword"
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "documentSection": {
                "properties": {
                    "largeDocumentModule": {
                        "properties": {
                            "largeDocs": {
                                "properties": {
                                    "typeAbbrev": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "hasProtocol": {
                                        "type": "boolean"
                                    },
                                    "hasSap": {
                                        "type": "boolean"
                                    },
                                    "hasIcf": {
                                        "type": "boolean"
                                    },
                                    "date": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "uploadDate": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "filename": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword"
                                    },
                                    "size": {
                                        "type": "integer"
                                    },
                                    "label": {
                                        "type": "text"
                                    }
                                }
                            },
                            "noSap": {
                                "type": "boolean"
                            }
                        }
                    }
                }
            }
        }

        return mapping