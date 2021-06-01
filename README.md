# Thesis Code and Output
## Github repository for thesis code and output. No data.

## Description of each script, ordered by run order:

- **load-data**: Downloads raw Profile, Persona, and Sales data from Sana SQL server, e.g. Opportunities, Contacts, Accounts, Funnel Dates, Activities, Activity Types, Activity Attributes, Sales activities, Sales Event Visits. Marketo Activities data is loaded from Azure Blob Storage.

- **o-c-a-filter-merge**: Merges Opportunity data with relevant contact and account data after cleaning and processing to create main data. Creates Profile and Persona Features. Creates outcome variable QO. 

- **sales activities**: Loads sales activities and event visits. Processes and merges to main data.

- **activities-pivot-merge**: Loads activity data, cleans and processes. Creates activity characteristics such as recency, content type, etc. Pivots by selected characteristic and merges with main data.

- **hasSaleshasMarketo**: Used for creating binary Marketo and Sales features. Modeling data is ready after this step.

- **preliminary analysis**: Used for exploratory analysis. 

- **model-1**: Tunes and trains XGBoost model, saves results.

- **model-2-markdown**: Reads model results and auto-generates file with output. Includes performance metrics, feature importance, partial dependence plots, SHAP summary plot.

- **compare-models**: Reads model results for chosen models, generates file with output to compare models.

- **thesis**: Additional code for exploratory analysis. 


