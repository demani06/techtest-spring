# techtest-spring

## To compile the application and run the tests
```gradle clean test```

## To run the application
```gradle bootRun```

## Few Caveats
* Async is added for the Spring boot application to submit the request to save to datalake asynchrnously
* @Retryable is used on the DataLake method call so that it retries upto 5 times (configurable number)
