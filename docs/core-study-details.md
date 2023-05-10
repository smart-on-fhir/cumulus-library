---
title: Core Study Details
parent: Library
nav_order: 4
# audience: clinical researchers, IRB reviewers
# type: reference
---

# Core study details

The core study calculates the **patient count** for every patient group using the [SQL CUBE function](https://prestodb.io/docs/current/sql/select.html#group-by-clause).  
*THRESHOLDS* are applied to ensure *no patient group has < 10 patients*.
 
Patient count can be the 
- number of **unique patients**
- number of **unique patient encounters**
- number of **unique patient encounters with documented medical note**
- Other types of counts are also possible, these are the most common.

Example

    #count (total) =
    #count patients age 9 =
    #count patients age 9 and rtPCR POS =
    #count patients age 9 and rtPCR NEG =
    #count rtPCR POS =
    #count rtPCR NEG =

[SQL CUBE](https://prestodb.io/docs/current/sql/select.html#group-by-clause) produces a "[Mathematical Power Set](http://en.wikipedia.org/wiki/Power_set)" for every patient subgroup.  
These numbers are useful inputs for maths that leverage [Joint Probability Distributions](https://en.wikipedia.org/wiki/Joint_probability_distribution). 

Examples: 

- [Odds Ratio](https://en.wikipedia.org/wiki/Odds_ratio) of patient group A vs B 
- [Relative Risk Ratio](https://en.wikipedia.org/wiki/Relative_risk) of patient group A vs B
- [Chi-Squared Test](https://en.wikipedia.org/wiki/Chi-squared_test) significance of difference between patient groups
- [Entropy and Mutual Information](https://en.wikipedia.org/wiki/Mutual_information) (core information theory measures) 
- [Decision Tree](https://en.wikipedia.org/wiki/Decision_tree) sorts patients into different predicted classes, with visualized tree   
- [Naive Bayes Classifier](https://en.wikipedia.org/wiki/Naive_Bayes_classifier) very fast probabilistic classifier
- others

## Core study exportable counts tables

### count_core_condition_icd10_month
| Variable  |   Description |
| --------  |   --------    |
| cnt   |   Count   |
| cond_month    |   Month condition recorded    |
| cond_code_display |   Condition code  |
| enc_class_code    |   Encounter Code (Healthcare Setting) |


### count_core_documentreference_month
| Variable  |   Description |
| --------  |   --------    |
| cnt   |   Count   |
| author_month  |   Month document was authored |
| enc_class_code    |   Encounter Code (Healthcare Setting) |
| doc_type_display  |   Type of Document (display)  |


### count_core_encounter_day
| Variable  |   Description |
| --------  |   --------    |
| cnt   |   Count   |
| enc_class_code    |   Encounter Code (Healthcare Setting) |
| start_date    |   Day patient encounter started   |


### count_core_encounter_month
| Variable  |   Description |
| --------  |   --------    |
| cnt   |   Count   |
| enc_class_code    |   Encounter Code (Healthcare Setting) |
| start_month   |   Month patient encounter started |
| age_at_visit  |   Patient Age at Encounter    |
| gender    |   Biological sex at birth |
| race_display  |   Patient reported race   |
| postalcode3   |   Patient 3 digit zip |


### count_core_observation_lab_month
| Variable  |   Description |
| --------  |   --------    |
| cnt   |   Count   |
| lab_month |   Month of lab result |
| lab_code  |   Laboratory Code |
| lab_result_display    |   Laboratory result   |
| enc_class_code    |   Encounter Code (Healthcare Setting) |


### count_core_patient
| Variable  |   Description |
| --------  |   --------    |
| cnt   |   Count   |
| gender    |   Biological sex at birth |
| age   |   Age in years calculated since DOB   |
| race_display  |   Patient reported race   |
| postalcode3   |   Patient 3 digit zip |
