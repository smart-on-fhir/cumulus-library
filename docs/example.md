---
title: Creating an example study
parent: Library
nav_order: 3
# audience: clinical researcher new to the project
# type: tutorial
---

# Creating an example study

By going through this example, we'll end up with a runnable library study, and we'll
talk through some of the question you'd go through in formulating a population survey.
We won't go into all the features here; just the ones you'd most commonly need.

When we're done with this, we'll have recreated the
[example study](https://github.com/smart-on-fhir/cumulus-library/tree/main/cumulus_library/studies/example).

You can run this on your own dataset, or you can run it on a synthetic data set, like the
[Sample bulk FHIR datasets](https://github.com/smart-on-fhir/sample-bulk-fhir-datasets/).
This guide assumes you just have a folder with FHIR ndjson in it, like the kind you'd
get from the sample datasets or from
[SMART Fetch](https://docs.smarthealthit.org/cumulus/fetch/),
but if you've already gotten data into Athena with
[Cumulus ETL](https://docs.smarthealthit.org/cumulus/etl/),
we'll provide alternate instructions for that when we get to that step.

## Prerequisites

You'll need to do the following steps to get things set up for running studies:

- Install [python](https://www.python.org/) if you haven't already, version 3.11 or later
- Install cumulus library with `python -m pip install cumulus-library`
- Make a directory called `example` in a location of your choice to hold the files
  we'll be using for this. Create an empty file in that folder named `__init__.py`
  - If you don't have a dataset already, get the 
    [synthetic 1000 patient data set](https://github.com/smart-on-fhir/sample-bulk-fhir-datasets/archive/refs/heads/1000-patients.zip),
    and unzip it to a folder named `1000-patients` in example.

### DuckDB
We'll want to run the `core` study on the dataset, to flatten the FHIR data out to make
it easier to inspect. So, we'll run the following command:
```bash
cumulus-library build --target core \
  --db-type duckdb \
  --database /path/to/your/example/duck.db \
  --load-ndjson-dir /path/to/your/example/1000-patients
```
To view the database, you can run `duckdb /path/to/your/example/duck.db -ui` to open a 
duckdb viewer in your web browser. You can click on the plus next to Notebooks to get a new notebook.
Then, you can click on 'add cell', and a cell will be added you can write queries in. Just click
the arrow in the upper left to run a query. Notebooks are saved between sessions. 


### Athena
If you have data in Amazon Athena, you can build the `core` study with the following command:
```bash
cumulus-library build --target core \
  --database your-database-name \
  --profile aws-profile-name \
  --region your-aws-region \
  --workgroup workgroup-name
```
If you don't know the values for profile/region/workgroup, talk to the person managing your account.

To view the database, you can log into the AWS console, and type 'Athena' in the search bar.
From there, select 'query your data in Athena console'. Make sure the workgroup in the dropdown
on the right matches the workgroup you used in the command line argument, and that you've selected
your matching database from the list of databases on the left. From there, you can write queries in
the central pane, and click the run button directly below the query to see the results.

## Step 0: What question are you asking?

Here are some examples of questions that we've used Cumulus to answer in the past:

- Can I predict a patient having a disease from a set of symptoms?
- Did patients with a specific medical condition have any adverse drug events after a specific treatment?
- Do coding systems accurately capture the prevalence of a particular disease?
- For a given patient, how similar are they to other patients with a similar disease?

A good question for a Cumulus study is one that involves sampling a patient population to create
cohorts over which you can run aggregate statistics. 

For our example here, we'll ask a simple question: *What kinds of medications are being prescribed
for preteens with bronchitis in the period from 2021-2026?* This question will work with the 
synthetic dataset, but it will also work just fine with real data.


## Step 1: Defining your study population

The first thing we're going to do is identify the total population that could be relevant to our
topic. There are four questions to consider here:

- *What kind of patients are relevant?* This is a demographics question. We might want to select
  patients by age, gender, area, or ethnographic group.
- *When were these patients in the medical system?* We might be looking for events in a specific time
  range, and we might have specific questions about their history
- *What was the setting of the visits?* We might want to focus on a specific type of patient setting,
  like emergency care, or we may want to look at patients that interacted with a specific department
- *How much care did the patient require?* We may be concerned about length of stay or followup visits
  after a treatment was provided

Looking at the above four questions, for our example question, two are definitely relevant: We are
looking for a kind of patient (<13 years of age), and a specific time range (the last 5 years).
We probably don't need to restrict on the setting, even though some settings are more likely than
others. We also don't need to restrict on healthcare utilization, since that's not in scope for the question we're asking.

So - lets look at the data to figure out how to apply these constraints. Let's start with the 
[Patient resource](https://hl7.org/fhir/us/core/STU4/StructureDefinition-us-core-patient.html) - 
open up your query editor and type in the following query:

```sql
SELECT * 
FROM core__patient 
LIMIT 10;
```

This shows a sample of the patients in the dataset. We want to get the `subject_ref` of patients who
are approximately 12 years old or less - though since we're looking over a five year window, we'll need
to factor in patients who were preteens at the time -  and there's a column named `birthdate`. So
let's filter by that. Run the following query:

```sql
SELECT 
  subject_ref, birthdate 
FROM core__patient 
WHERE date_diff('year', birthdate, now()) <(13 +5);
```

And with that, we've got a list of all patients matching our demographics.

Now lets do something similar with encounters - first, let's look at the 
[Encounter resource](https://hl7.org/fhir/us/core/STU4/StructureDefinition-us-core-encounter.html):

```sql
SELECT * 
FROM core__encounter 
LIMIT 10;
```
In this table, we've got `enounter_ref` to identify each encounter, as well as a `subject_ref`
that lines up with the patients we IDed above, and there's a `period_start_year` field we can
use to cutoff older encounters. So let's filter by that and see how these fields look:

```sql
SELECT
   encounter_ref, subject_ref, period_start_year
FROM core__encounter
WHERE date_diff('year', period_start_year, now()) < 5;
```

And that gets us all encounters in the last five years, and the patients involved.

So, now we're going to combine these two queries together to get the set of encounters for
the patients we've selected. We'll write a SQL join statement to identify just the encounters
for the group of patients we're interested in (and we'll add a check to make sure the patient
was the appropriate age at the time of the encounter):

```sql
SELECT
    p.subject_ref,
    p.birthdate,
    e.encounter_ref,
    e.period_start_year
FROM core__patient as p
INNER JOIN core__encounter as e ON p.subject_ref = e.subject_ref
WHERE
    date_diff('year', p.birthdate, now()) <(13 +5)
    AND date_diff('year', e.period_start_year, now()) < 5
    AND date_diff('year', p.birthdate, e.period_start_year) < 13;
```

We can look this over to check. The birthdate and period are just there for verification; the IDs
are the things we'll be using later.

Now that we're comfortable with the study population, we'll start creating our first files related
to the study. Every study has a name, and we'll need to know it for some of the next steps, so
we'll call our study `example`. 

{: .note }
A valid study name has no spaces and uses underscores to separate words.

In the directory we set up to hold our study, let's create a folder called `queries`. In that folder,
we'll create a file called `study_population.sql`. We'll wrap the query we used above in a create
table statement to make a table named `example__study_population`. We separate the study
prefix from the subject of the table with a double underscore.

```sql
CREATE TABLE example__study_population AS (
    SELECT
        p.subject_ref,
        p.birthdate,
        e.encounter_ref,
        e.period_start_year
    FROM core__patient AS p
    INNER JOIN core__encounter AS e ON p.subject_ref = e.subject_ref
    WHERE
        date_diff('year', p.birthdate, now()) <(13 +5)
        AND date_diff('year', e.period_start_year, now()) < 5
        AND date_diff('year', p.birthdate, e.period_start_year) < 13
);
```

With that done, back in the root directory of the study, we'll create a *manifest*. This contains a list
of all the files in a study. We're going to define the study prefix, and then add an action that our
study should take to build the above table. Let's create a file named `manifest.toml` in our `example`
directory, with the following contents:

```toml
study_prefix = "example"
description = "An example study looking at medication usage by preteens with bronchitis"
[[stages.default]]
label = "study population tables" # this is shown when the study is being built
files = [ "queries/study_population.sql" ]
```

{:.note } 
The `[[stages.default]]` syntax indicates that we're adding the data below it to a list.
We'll use this pattern repeatedly to add actions to our study.

Let's go ahead and build our study, just to make sure that everything is working correctly.

- For DuckDB, note that
  - we don't need to reference the NDJSON folder now that we're working from core tables
  - if you're running the duckdb UI, you'll need to shut it down before running this command
    by typing `.exit` in the terminal:
```bash
cumulus-library build --target example \
  --study-dir /path/to/your/example/ \
  --db-type duckdb \
  --database /path/to/your/example/duck.db
```

- For Athena:
```bash
cumulus-library build --target example \
  --study-dir /path/to/your/example/ \
  --database your-database-name \
  --profile aws-profile-name \
  --region your-aws-region \
  --workgroup workgroup-name
```

## Step 2: Defining cohorts & selecting resources

Now that we have our population, we're going to slice it into *cohorts*. A cohort is slice of our
study population, matching against one ore more critera. A study can have more than one cohort,
and we might want to have more than one here. Specifically, the following groupings may interesting:

- All patients with a diagnosis related to bronchitis
- All patients taking a specific medication
- All patients taking a medication with a specific ingredient

Going back to our research question, *What kinds of medications are being prescribed
for preteens with bronchitis in the period from 2021-2026?*, the first one grouping is probably the most
relevant. If we were to expand the scope of our question to look at effects over time (making
our research question more like *How effective are medications in treating preteens with
bronchitis?*), we might consider adding the medication/ingredient specific cohorts. We'll skip
that for now to keep our example study simple, but we'll come back to this idea at the end of
this guide when we talk about potential next steps.

In order to select patients by diagnosis, we'll need data about how diagnoses are applied, so
we'll want to include a new resource type in our study, 
[Condition](https://hl7.org/fhir/us/core/STU4/StructureDefinition-us-core-condition.html) - we can
use that to slice our population into a bronchitis cohort. We also want to include data from
[Medication Requests](https://hl7.org/fhir/us/core/STU4/StructureDefinition-us-core-medicationrequest.html)
so we have a source for that data. So we're going to do two things:

- Select our cohort by Condition
- Generate study tables for all of the resources we're interested in for all members of the cohort.

### Uploading data

How are we going to specify what conditions we're interested in, though? We have a convenient helper
for doing this kind of thing - we call these 
[*workflows*](workflows.md).
The one we're interested in for this case is the file upload workflow. This lets us take tabular data
and use it to create a table. With this idea in hand, let's do an exploratory query against our
data:

```sql
SELECT DISTINCT(code_display), code, "system", count(*) AS 'num' 
FROM core__condition 
GROUP BY code_display, code, "system"
ORDER BY num DESC LIMIT 100;
```

This shows us the top 100 conditions in the dataset, along with the code and coding system used
to populate the data. If you're using the sample FHIR dataset, you'll notice that conditions are
all encoded using the SNOMED coding system. That's likely the case for your dataset as well, but
if your data is encoded differently, you may need to adjust the following step to match your
data more closely.

Since we're looking at SNOMED codes, we are going to make a quick csv that contains all the codes
related to bronchitis. NIH provides several tools for exploring these datasets - we'll talk about
some of the options here more in the next section, but for the purposes of this example, we'll use
[EVS Explore](https://evsexplore.semantics.cancer.gov/evsexplore/concept/snomedct_us/32398004),
which does not require any special login information. Here we can see Bronchitis defined,
along with all its children - this provides a nice valueset of codes for us to use.

Let's create a table based on this valueset. In our study directory, let's make a new folder,
`datasets`, and inside it, we'll make a new file, `snomed_bronchitis.csv`:

```csv
code,display_name,system
32398004,bronchitis,http://snomed.info/sct
10509002,Acute bronchitis,http://snomed.info/sct
405720007,Allergic bronchitis,http://snomed.info/sct
405944004,Asthmatic bronchitis,http://snomed.info/sct
846638002,Bronchitis caused by chemical fumes,http://snomed.info/sct
10625791000119101,Bronchitis caused by chemical,http://snomed.info/sct
846639005,Bronchitis caused by vapor,http://snomed.info/sct
785728005,Bronchitis co-occurrent with wheeze,http://snomed.info/sct
396285007,Bronchopneumonia,http://snomed.info/sct
89549007,Catarrhal bronchitis,http://snomed.info/sct
63480004,Chronic bronchitis,http://snomed.info/sct
445058002,Inflammation of bronchus caused by Aspergillus,http://snomed.info/sct
53926002,Plastic bronchitis,http://snomed.info/sct
40600002,Pneumococcal bronchitis,http://snomed.info/sct
713175006,Postoperative bronchitis,http://snomed.info/sct
29591002,Purulent bronchitis,http://snomed.info/sct
65878001,Septic bronchitis,http://snomed.info/sct
36426008,Subacute bronchitis,http://snomed.info/sct
13617004,Tracheobronchitis,http://snomed.info/sct
186178000,Tuberculosis of bronchus,http://snomed.info/sct
16146001,Viral bronchitis,http://snomed.info/sct
```

And now we'll configure a workflow to create a table from the valueset. The
[File upload workflow](workflows/file-upload.md)
has several commands to handle lots of use cases, but we're using the most basic one - we have a csv,
it has headers, and we don't need to worry about complex types, so our workflow will be very simple.
Create a new folder, `workflows`, and inside it, create a file named `upload.workflow`. We'll put
the following config in that file:

```toml
config_type="file_upload"

[tables.snomed_bronchitis]
file = "../datasets/snomed_bronchitis.csv"
```

This workflow will create a table named `example__snomed_bronchitis` for us. Let's add an
action to our manifest.toml to run this workflow:

```toml
study_prefix = "example"

[[stages.default]]
label = "study population tables" # this is shown when the study is being built
files = [ "queries/study_population.sql" ]

[[stages.default]]
label = "upload valuesets"
files = [ "workflows/upload.workflow" ]
```

We can then run this to make sure we've configured everything correctly:
- For DuckDB:
  ```bash
cumulus-library build --target example \
  --study-dir /path/to/your/example/ \
  --db-type duckdb \
  --database /path/to/your/example/duck.db
  ```

- For Athena:
  ```bash
cumulus-library build --target example \
  --study-dir /path/to/your/example/ \
  --database your-database-name \
  --profile aws-profile-name \
  --region your-aws-region \
  --workgroup workgroup-name
  ```
With our valueset in place, we can start using it to define our cohort. Let's run the following query
to see what conditions match our valueset, as well as our study population:

```sql
SELECT c.*
FROM core__condition AS c
INNER JOIN example__snomed_bronchitis AS sb 
  ON c.code = sb.code AND c.system = sb.system
INNER JOIN example__study_population AS sp
  ON c.subject_ref = sp.subject_ref;
```

This returns a few hundred rows on the sample dataset, so we're good to go. Let's add a new query
to the `queries` folder , named `bronchitis_condition.sql` (we're going to use 
bronchitis from here on out as a prefix for files & tables to distinguish what's in that cohort:

```sql
CREATE TABLE example__bronchitis_condition AS (
  SELECT DISTINCT c.*
  FROM core__condition AS c
  INNER JOIN example__snomed_bronchitis AS sb 
    ON c.code = sb.code AND c.system = sb.system
  INNER JOIN example__study_population AS sp
    ON c.subject_ref = sp.subject_ref
);
```

And we'll add a new action to our manifest to handle building this table:

```toml
study_prefix = "example"

[[stages.default]]
label = "study population tables" # this is shown when the study is being built
files = [ "queries/study_population.sql" ]

[[stages.default]]
label = "upload valuesets"
files = [ "workflows/upload.workflow" ]

[[stages.default]]
label = "bronchitis cohort definition"
files = [ "queries/bronchitis_condition.sql" ]
```

If we like, we can rebuild the study at this point to check using the same commands as mentioned above.

With a table containing our cohort, we can use  the patient IDs in that table to include the relevant
data from the other resource tables we're interested in. This is fairly boilerplate, so we'll just 
have three very similar looking queries we'll create in the `queries` folder, but if you'd like to
check anything against the database, you can run the queries by just using the portion between the
parenthesis at the start and end of the query.

`bronchitis_patient.sql`:
```sql
CREATE TABLE example__bronchitis_patient AS (
  SELECT DISTINCT p.*
  FROM core__patient AS p
  INNER JOIN example__bronchitis_condition AS c
    ON c.subject_ref = p.subject_ref
);
```

`bronchitis_encounter.sql`
```sql
CREATE TABLE example__bronchitis_encounter AS (
  SELECT DISTINCT e.*
  FROM core__encounter AS e
  INNER JOIN example__bronchitis_condition AS c
    ON c.subject_ref = e.subject_ref
);
```

`bronchitis_medicationrequests.sql`
```sql
CREATE TABLE example__bronchitis_medicationrequest AS (
  SELECT DISTINCT mr.*
  FROM core__medicationrequest AS mr
  INNER JOIN example__bronchitis_condition AS c
    ON c.subject_ref = mr.subject_ref AND c.encounter_ref = mr.encounter_ref
);
```

Now we're going to add these to the manifest, but we're going to do it in a slightly different
way. None of these tables depend on each other - they're all looking at the bronchitis condition table,
but they have no inter-dependencies. So we're going to add a new element to the action, indicating
that these can be executed in parallel, and we'll list multiple files in that step. This will allow these tables to build much faster than they would in series. 

Our prior steps all ran files in order - if we wanted to, we could combine some of those actions.
Let's combine the valueset upload and the cohort table, just to get an example of what that looks like.

So our manifest now looks like this:

```toml
study_prefix = "example"

[[stages.default]]
label = "study population tables"
files = [ "queries/study_population.sql" ]

[[stages.default]]
label = "upload valuesets & define cohort"
files = [ 
    "workflows/upload.workflow",
    "queries/bronchitis_condition.sql"
]

[[stages.default]]
label = "bronchitis cohort additional resources"
type = "build:parallel"
files = [ 
    "queries/bronchitis_encounter.sql",
    "queries/bronchitis_medicationrequest.sql",
    "queries/bronchitis_patient.sql",
]
```

So just to recap these changes, the next time we build, this will:

- Run the study population table with its associated label
- Run the upload workflow and the cohort condition creation, in order, with their associated label
- Run the remaining resource tables in parallel, with the same label.

We'll run a build of our study once more to make sure the tables are all created successfully.

With that, we've defined our cohort and selected only the relevant resources. Now we're ready
to move on to the next step.

## Step 2.5: Extract features from notes

Since this example is simple, we're not going to do this here, but it's worth noting that if
a part of our study involved getting data from notes using natural language processing, we'd
do that step here. We'll circle back on this at the end of this guide.

## Step 3: Analysis & Population metrics

Let's step back to our research question now: 

*What kinds of medications are being prescribed for preteens with bronchitis in the last five years?*

We have a cohort of those patients, which includes their encounters and their medications. What
can we do with that info? 

The cumulus ecosystem provides some different visualization tools for viewing anonymized population
data, and so to a certain extent, we don't need to worry about that just yet - if we can identify
interesting data points, we can create an export that is loadable into those tools, and we can do our
initial analysis there. But we have to massage the data before we create that export.

Here are the kinds of things studies have done in the past at that stage:

- If display names are messy or missing in the source data, we can replace them with data from a
  coding system, so that it's easier to bin like things together
- We can create new features from the source data - like grouping medications by ingredient, or
  grouping symptoms as potential indicators of a condition
- We can sample a cohort to do statistical tests, or train a model on the sample and validate it on the
  held out population
- If we've used NLP to extract features, we can validate them against a set of human annotators for
  correctness

Let's create a new feature from this dataset. To start, let's look at the cohort's medication
requests. Run this query to get a list of distinct medicines used in the cohort:

```sql
SELECT DISTINCT
    medication_code,
    medication_display 
FROM example__bronchitis_medicationrequest
ORDER BY medication_code;
```

If you're using the sample bulk FHIR dataset, you'll see five medications listed:

```csv
medication_code, medication_display
1043400, Acetaminophen 21.7 MG/ML / Dextromethorphan Hydrobromide 1 MG/ML / doxylamine succinate 0.417 MG/ML Oral Solution
313782, Acetaminophen 325 MG Oral Tablet
349094, budesonide 0.125 MG/ML Inhalation Suspension
630208, albuterol 0.83 MG/ML Inhalation Solution
895996, 120 ACTUAT fluticasone propionate 0.044 MG/ACTUAT Metered Dose Inhaler [Flovent]
```

Two of these are over the counter oral medications, and three are prescription inhalers. We could
create a feature capturing this distinction. Let's add a new file to our `datasets` folder, 
`med_delivery_mechanism.csv`, where we label the codes by the delivery mechanism:

```csv
medication_code, medication_display, delivery_mechanism
1043400, Acetaminophen 21.7 MG/ML / Dextromethorphan Hydrobromide 1 MG/ML / doxylamine succinate 0.417 MG/ML Oral Solution, Oral
313782, Acetaminophen 325 MG Oral Tablet, Oral
349094, budesonide 0.125 MG/ML Inhalation Suspension, Inhaled
630208, albuterol 0.83 MG/ML Inhalation Solution, Inhaled
895996, 120 ACTUAT fluticasone propionate 0.044 MG/ACTUAT Metered Dose Inhaler [Flovent], Inhaled
```

Now we need to add this to a file upload workflow. We could chain this together with our previous
upload, but we won't for the following reason - since we've already defined our cohort, we may want
to preserve it rather than rerunning - every time you re-run a study, you're removing old tables
and replacing them with new ones, and if the underlying data changes, the shape of your cohort can
also change. 

Since we're in the analysis phase, we're going to make use of the concept of a *stage* - we've been
doing this already, every time we've used `[[stages.default]]` in the manifest. A study can support
more than one stage, and only cleans up the tables associated with the stage you're running. So, let's
add a new stage, `analysis`, and add a file upload workflow there as well.

In the workflows folder, let's create a new file, name `upload_analysis.workflow`, with the following
contents:

```toml
config_type="file_upload"

[tables.med_delivery_mechanism]
file = "../datasets/med_delivery_mechanism.csv"
col_types = ["STRING","STRING","STRING"]
```

And then we'll update the manifest to reference that stage:

```toml
study_prefix = "example"

[[stages.default]]
label = "study population tables"
files = [ "queries/study_population.sql" ]

[[stages.default]]
label = "upload valuesets & define cohort"
files = [ 
    "workflows/upload.workflow",
    "queries/bronchitis_condition.sql"
]

[[stages.default]]
label = "bronchitis cohort additional resources"
type = "build:parallel"
files = [ 
    "queries/bronchitis_encounter.sql",
    "queries/bronchitis_medicationrequest.sql",
    "queries/bronchitis_patient.sql",
]

[[stages.analysis]]
label = "Postprocess data for analysis"
files = [ 
    "workflows/upload_analysis.workflow"
]
```

To run this, we're going to add a `--stage` command to the arguments we've been using up until this
point:

- For DuckDB:
  ```bash
cumulus-library build --target example --stage analysis\
  --study-dir /path/to/your/example/ \
  --db-type duckdb \
  --database /path/to/your/example/duck.db
  ```

- For Athena:
  ```bash
cumulus-library build --target example --stage analysis\
  --study-dir /path/to/your/example/ \
  --database your-database-name \
  --profile aws-profile-name \
  --region your-aws-region \
  --workgroup workgroup-name
  ```

Now we can tweak our output analysis without touching the contents of the cohort.

So, let's think about what kinds of output tables would be useful to create. Some easy to check off
items:

- A summary of the patient demographics
- A distribution of when the encounters occurred
- A summary of the medications
- A table combining the patient demographics with the medications used, including our feature

The first four can be done from the cohort tables we've already created. The last one involves us
doing some more complex logic. So let's write a query to prep the data for us.

When writing this kind of query, it can be helpful to look at summaries of the data to figure out
what we'd like to include. So you may want to run the following queries:

```sql
SELECT * 
FROM example__bronchitis_encounter
LIMIT 10;
```
```sql
SELECT * 
FROM example__bronchitis_patient
LIMIT 10;
```
```sql
SELECT * 
FROM example__bronchitis_medicationrequest
LIMIT 10;
```

Inspecting these tables, here's what our combined table needs to do:

- Get the patient demographics
- Use the encounter and the patient's DOB to calculate the age at visit
- Get the medication code associated with the encounter
- Annotate the medication data with the feature we created

We'll join together the first three in our new table. The annotation we can handle easily at a later
stage. The following query, which we'll save as `queries/bronchitis_meds_by_patient.sql` performs this with a multi stage select, with left joins, ensuring we
get cases where a patient had a condition but was not prescribed a medication:

```sql
CREATE TABLE example__bronchitis_meds_by_patient AS (
    WITH step_1 as (
        SELECT 
            e.subject_ref,
            e.encounter_ref,
            e.age_at_visit,
            p.gender,
            p.race_display,
        FROM example__bronchitis_encounter AS e
        LEFT JOIN example__bronchitis_patient AS p
        ON p.subject_ref = e.subject_ref
        order by e.subject_ref, e.encounter_ref
    )
    SELECT
        s.subject_ref,
        s.encounter_ref,
        s.gender,
        s.race_display,
        s.age_at_visit,
        m.medication_code
        FROM step_1 AS s
        LEFT JOIN example__bronchitis_medicationrequest AS m
        ON s.subject_ref = m.subject_ref
            AND s.encounter_ref = m.encounter_ref
);
```

And now we'll add this file to our action in the analysis stage:

```toml
study_prefix = "example"

[[stages.default]]
label = "study population tables"
files = [ "queries/study_population.sql" ]

[[stages.default]]
label = "upload valuesets & define cohort"
files = [ 
    "workflows/upload.workflow",
    "queries/bronchitis_condition.sql"
]

[[stages.default]]
label = "bronchitis cohort additional resources"
type = "build:parallel"
files = [ 
    "queries/bronchitis_encounter.sql",
    "queries/bronchitis_medicationrequest.sql",
    "queries/bronchitis_patient.sql",
]

[[stages.analysis]]
label = "Postprocess data for analysis"
files = [ 
    "workflows/upload_analysis.workflow",
    "queries/bronchitis_meds_by_patient.sql"
]
```

Now we're ready to generate some counts. There's a 
[workflow](workflows/counts.md)
for this as well, so we'll use that - for most of our tables, we'll just need to say what columns
we want to include. We'll use an annotation to add display values based on the underlying code
system for our meds by patient table.

In the `workflows` folder, create a new file, `counts.workflow`. We'll put all of our counts table
definitions in that file:

```toml
config_type = "counts"


[tables.count_bronchitis_encounter]
source_table = "example__bronchitis_encounter"
description = """Encounters of patients with a diagnosis of bronchitis"""
table_cols = [
    "period_start_month",
    "age_at_visit",
    "class_display",
    "type_display"
]
min_subject=2
secondary_id="encounter_ref"

[tables.count_bronchitis_medicationrequest]
source_table = "example__bronchitis_medicationrequest"
description = """Details about requests for medication for patients with bronchitis"""
table_cols = [
    "authoredon_month",
    "medication_code",
    "medication_display",
]
min_subject=2

[tables.count_bronchitis_patient]
source_table = "example__bronchitis_patient"
description = """Demographic details about patients with bronchitis"""
table_cols = [
    "gender",
    "birthdate",
    "race_display",
    "ethnicity_display"
]
min_subject=2

[tables.count_bronchitis_meds_by_patient]
source_table = "example__bronchitis_meds_by_patient"
description = """Details about medications for bronchitis with patient demographic details"""
table_cols = [
    "gender",
    "age_at_visit",
    "race_display",
    "medication_code",
]
min_subject=2

[tables.count_bronchitis_meds_by_patient.annotation]
field = "medication_code"
join_table = "example__med_delivery_mechanism"
join_field = "medication_code"
columns = [
    ["medication_display", "varchar"],
    ["delivery_mechanism", "varchar"]
]
```

Note that we're setting the min subject value to 2, with the assumption that we're looking at the bulk
FHIR synthetic data, which is generally small. In practice for real datasets, we'd leave this unset,
causing the default bin size of 10 to be used.

## Step 4: Exporting data

Now that we've got our analysis, we want to export it from the database so that we can push it into
analytics tools, like the Cumulus dashboard. 

We'll need to do one additional bit of table management - there are two metadata tables we need
to create to help describe the study. We need a `example__meta_date` table, describing the
date range of data selected in the study, and a `example__meta_version` table, which our
downstream tools will use for grouping like kinds of data. Let's add these tables to the `queries`
folder now.

`queries/meta_date.sql`:
```sql
CREATE TABLE example__meta_date AS
SELECT
    min(period_start_day) AS min_date,
    max(period_end_day) AS max_date
FROM example__bronchitis_encounter;
```

`queries/meta_version.sql`:
```sql
CREATE TABLE example__meta_version AS
SELECT 1 AS data_package_version;
```

And with those in place, we're going to make a few additional modifications to our manifest:

- We'll add those new queries to one of our actions
- We'll add export actions to the analysis stage, targeting all the count tables we created
- We'll add a description to the manifest, which will describe the study at a high level, which
  can be used in downstream tooling to convey info about what the data represents

With those additions, our manifest looks like this:

```toml
study_prefix = "example"
description = """A example study showing usage of bronchitis medication in preteens.default

This study was designed to run against the 1000 patient sample bulk FHIR dataset. It's
intended to show how building a study works, while providing a realistic use case for
anchoring the clinical context."""

[[stages.default]]
label = "study population tables"
files = [ "queries/study_population.sql" ]

[[stages.default]]
label = "upload valuesets & define cohort"
files = [ 
    "workflows/upload.workflow",
    "queries/bronchitis_condition.sql"
]

[[stages.default]]
label = "bronchitis cohort additional resources"
type = "build:parallel"
files = [ 
    "queries/bronchitis_encounter.sql",
    "queries/bronchitis_medicationrequest.sql",
    "queries/bronchitis_patient.sql",
]

[[stages.analysis]]
label = "Postprocess data for analysis"
files = [ 
    "workflows/upload_analysis.workflow",
    "queries/bronchitis_meds_by_patient.sql",
    "queries/meta_date.sql",
    "queries/meta_version.sql",
]

[[stages.analysis]]
label = "Generate summary counts"
type = "build:parallel"
files = [ 
    "workflows/counts.workflow",
]

[[stages.analysis]]
label = "Export count tables"
type = "export:counts"
tables = [ 
    "example__count_bronchitis_encounter",
    "example__count_bronchitis_medicationrequest",
    "example__count_bronchitis_patient",
]

[[stages.analysis]]
label = "Export count tables"
type = "export:annotated_counts"
tables = [ 
    "example__count_bronchitis_meds_by_patient",
]
```

With that set up, we can now run cumulus-library's export mode to download the counts and metadata
as a zip file.

- For DuckDB:
  ```bash
cumulus-library export --target example --stage analysis\
  --study-dir /path/to/your/example/ \
  --db-type duckdb \
  --database /path/to/your/example/duck.db \
  /path/to/your/export/dir
  ```

- For Athena:
  ```bash
cumulus-library export --target example --stage analysis\
  --study-dir /path/to/your/example/ \
  --database your-database-name \
  --profile aws-profile-name \
  --region your-aws-region \
  --workgroup workgroup-name \
  /path/to/your/export/dir
  ```

In the directory you specify for export, you should have some csvs of your count tables, and a zip file
with parquet files ready to upload to the
[Cumulus Aggregator](sharing-data.md)
if you're participating in a federated study.

## Troubleshooting

Hitting some issues? Here's some things to check:

- Did you get a message in red containing a string like `Object contains unknown field`? This means
  that your manifest has an incorrect key. It should mention a field name - look for that field in
  the manifest and correct it to one of the expected values.
- Did something happen and you're not sure what state the study is in? You can always reset it and
  start again using the `clean` mode.
  - For DuckDB:
```bash
cumulus-library clean --target example --stage all \
  --study-dir /path/to/your/example/ \
  --db-type duckdb \
  --database /path/to/your/example/duck.db \
```

  - For Athena:
```bash
cumulus-library clean --target example --stage all \
  --study-dir /path/to/your/example/ \
  --database your-database-name \
  --profile aws-profile-name \
  --region your-aws-region \
  --workgroup workgroup-name \
  /path/to/your/export/dir
```
- Are you running into a SQL query error? Those are tricky to debug, but usually you'll get a line
  number as a reference to start looking aroubnd for syntax errors.


## Next steps

This guide covered basic authoring and configuration of a study - there's more things you can look
at from here.

- We mentioned valuesets and looking up medications - similar to how we defined a valueset by hand,
  we provide a
  [workflow](workflows/valueset.md)
  for creating valuesets, either directly from
  [UMLS](https://www.nlm.nih.gov/research/umls/index.html)
  (which requires an API key), or from a hand curated list of keywords run against the
  [Cumulus UMLS study](github.com/smart-on-fhir/cumulus-library-umls)
  (which uses the same API key).
- We skipped over NLP, but that's a core feature of cumulus. You can read about that process in our
  [Example NLP Workflow](docs.smarthealthit.org/cumulus/nlp/example.html)
- There's a comprehensive 
  [study configuration guide](study_configuration.md)
  that exhaustively covers all the configuration options in the manifest
- If you have any questions, or would like any clarifications/additions to this guide, you can reach
  us on our
  [discussion forum](https://github.com/smart-on-fhir/cumulus/discussions)