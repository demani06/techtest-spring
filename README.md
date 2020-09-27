# techtest-spring

## To compile the application and run the tests\
```gradle clean test```

## To run the application\
```gradle bootRun```

## Few Caveats
* Async is added for the Spring boot application to submit the request to save to datalake asynchrnously
* The actual implementation to save the digest is not done due to the time constraints
