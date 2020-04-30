# CovidIA - Data ingestion & processing

This repository gathers the Azure Functions implemented to feed the CovidIA application with updated data.

## Function definition
The following Azure Functions are defined in this repository. _Additional functions should not be necessary_

#### Cron Daily
- **Type** : `TimeTrigger`
- **Schedule** : `0 0 21 * * *` Every day at 9 PM
- **Task** : Launch `ingestion_daily`

#### Cron Weekly
- **Type** : `TimeTrigger`
- **Schedule** : `0 0 1 * * 6` Every Saturnday at 1 AM
- **Task** : Launch `ingestion_weekly`

#### Ingestion Daily
- **Type** : `HttpTrigger`
- **Task** : Ingest **daily** updated data from various sources

#### Ingestion Weekly
- **Type** : `HttpTrigger`
- **Task** : Ingest **weekly** updated data from various sources

#### Processing Daily
- **Type** : `HttpTrigger`
- **Task** : Format and process **daily** ingested data to comply with back end application requirements

#### Processing Weekly
- **Type** : `HttpTrigger`
- **Task** : Format and process **weekly** ingested data to comply with back end application requirements

## Add new data sources
To add a new data source _(daily or weekly)_, you need to create a new object inherited from `libs.scrapper.Scrapper`. You can add your newly created scrapper to the `SCRAPPER_TO_RUN` list in ingestion function `__init__.py` file.

## Add new processing steps
To add a new processing _(daily or weekly)_, you need to create a new object inherited from `libs.processor.Processor`. You can add your newly created processor to the `PROCESSOR_TO_RUN` list in processing function `__init__.py` file.

## Appendix - TimerTrigger - Python

The `TimerTrigger` makes it incredibly easy to have your functions executed on a schedule. This sample demonstrates a simple use case of calling your function every 5 minutes.

### How it works

For a `TimerTrigger` to work, you provide a schedule in the form of a [cron expression](https://en.wikipedia.org/wiki/Cron#CRON_expression)(See the link for full details). A cron expression is a string with 6 separate expressions which represent a given schedule via patterns. The pattern we use to represent every 5 minutes is `0 */5 * * * *`. This, in plain text, means: "When seconds is equal to 0, minutes is divisible by 5, for any hour, day of the month, month, day of the week, or year".

### Learn more
- [CRONTAB Expression](https://docs.microsoft.com/en-us/azure/azure-functions/functions-bindings-timer?tabs=csharp#ncrontab-expressions)
- [Schedule a function with Azure](https://www.serverlessnotes.com/docs/scheduling-with-azure-functions)
