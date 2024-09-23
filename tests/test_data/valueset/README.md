# Test Data

This document describes the sources of the more complex datasets 
used by unit testing.

## rxnconso/rxnrel/rxnsty

These are subsets of the RxNorm sources pulled from the pure sql version of the
vocab lookup, which means that they generally align with the keywords being used
for searching. rnxrel.csv has been light modified, changing 'tradename_of' to
'reformulated to', which is clinically incorrect but does allow us to more
easily test drive the included rels table.

## vsac_resp

This is the API response from the expansion endpoint of the VSAC API, targeted
at the ACEP curated dataset of opioid medications.