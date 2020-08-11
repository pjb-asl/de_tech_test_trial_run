# Street Group: Data Engineer Interview Task

This is a trial run through the interview task

#### Requirements:

* Business Requirements:
  * Obtain the Land Registry Price Paid Transaction Data
  * Convert the CSV format to JSON
  * Group the transactions together by property (determine the key that we use to identify unique properties)
* Non functional Requirements:
  * Use Apache Beam
  * Use Python
  * Deploy to the cloud
  * Process 20+ Million Records
  * Include test coverage where necessary
  
#### Pipeline Architecture:

source_file.csv -> BeamPipeline:Transform to JSON -> Output File:transactions.json

transactions.json -> BeamPipeline:GroupByProperty -> property_transaction_history.json

#### Design Considerations:

Most of the heavy lifting will be done by Apache Beam. Some python validation code will be used to aid the transformation from csv to json AND the construction of a property key. This should be built with TDD so that there is test coverage moving forward.

Development will be done using constructed test data that reflects observations in the original source data.

The pipelines should handle transformation issues and park them somewhere for further investigation.

There will be two pipelines. I'm resisting the temptation to do this as a single end to end effort. Breaking it up reduces the complexity in each pipeline and it allows for an interim step that means replaying the following stages becomes easier.

The TDD library of choice is pytest. This is a batch processing pipeline that will be pushed into GCP Dataflow therefore a release pipeline isn't necessary until automation is valuable.

Remember: Avoid including libraries/modules that add complexity to the pipeline deployment!

#### Data Fields (Source File)

* tx_uid
* price
* tx_date
* postcode
* property_type
* age_classification
* tenure_duration
* paon
* saon
* street
* locality
* town_city
* district
* county
* ppd_category_type
* change_type

16 fields

#### Development Environment Configuration

I'm going to use a conda environment to isolate the python executable and installed modules

This was set up using pycharm.

