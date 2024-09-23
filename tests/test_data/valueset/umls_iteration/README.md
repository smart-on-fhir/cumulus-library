# Umls data mock structure

All data is lightly massaged data from the relevant source tables.

It is assumed that the query string for lookup is 'opioid' for system 'MED-RT'

## mrconso.csv

Lines 2-3 should be ignored due to having a different system
Line 4 should be found in the regex lookup, but have no related concepts
Line 5 should be found in the regex lookup, and have two related concepts
Lines 6-9 should be found in the tier 2 search
lines 10-13 should be found in the tier 3 search
Lines 14-16 should be found in the tier 4 search
Lines 17-19 should never be found, as an unrelated concept

## mrrel.csv

Lines 2-5 should be found in the tier 2, by matching
    the cui found via word search (C2917209) to the cui1 field in this table.
Lines 6-9 should be found in the tier 3 expansion search, by looking up
    all cui1 values that match a cui2 value in our tier 0 set that themselves
    have a cui2 value that is not yet in the dataset
lines 10-11 should be found in the tier 4 search     
Lines 12-13 are unrelated relations that should not be used.
