# Learning Google BigQuery
This is the code repository for [Learning Google BigQuery](https://www.packtpub.com/big-data-and-business-intelligence/learning-google-bigquery?utm_source=github&utm_medium=repository&utm_campaign=9781787288591), published by [Packt](https://www.packtpub.com/?utm_source=github). It contains all the supporting project files necessary to work through the book from start to finish.
## About the Book
Google BigQuery is a popular cloud data warehouse for large-scale data analytics. This book will serve as a comprehensive guide to mastering BigQuery, and how you can utilize it to quickly and efficiently get useful insights from your Big Data.

You will begin with getting a quick overview of the Google Cloud Platform and the various services it supports. Then, you will be introduced to the Google BigQuery API and how it fits within in the framework of GCP. The book covers useful techniques to migrate your existing data from your enterprise to Google BigQuery, as well as readying and optimizing it for analysis. You will perform basic as well as advanced data querying using BigQuery, and connect the results to various third party tools for reporting and visualization purposes such as R and Tableau. If you're looking to implement real-time reporting of your streaming data running in your enterprise, this book will also help you.

## Instructions and Navigation
All of the code is organized into folders. Each folder starts with a number followed by the application name. For example, Chapter02.



The code will look like the following:
```
SELECT year(pickup_datetime) as trip_year, count(1) as trip_count
FROM [nyc-tlc:yellow.trips]
```

For this book all you would require is the Google Cloud SDK, the browser of your choice
(Chrome is recommended), and an editor that supports PHP coding, and you're all set to
begin. It is also recommended to learn SQL basics for writing advanced queries in Google
BigQuery. This book uses Google BigQuery Public Dataset for demos.

## Related Products
* [Google Cloud Platform for Developers](https://www.packtpub.com/virtualization-and-cloud/google-cloud-platform-developers?utm_source=github&utm_medium=repository&utm_campaign=9781788837675)

* [Learn PowerShell - Fundamentals of PowerShell 6](https://www.packtpub.com/networking-and-servers/learn-powershell-fundamentals-powershell-6?utm_source=github&utm_medium=repository&utm_campaign=9781788838986)

* [Practical Deep Reinforcement Learning](https://www.packtpub.com/big-data-and-business-intelligence/practical-deep-reinforcement-learning?utm_source=github&utm_medium=repository&utm_campaign=9781788834247)

### Suggestions and Feedback
[Click here](https://docs.google.com/forms/d/e/1FAIpQLSe5qwunkGf6PUvzPirPDtuy1Du5Rlzew23UBp2S-P3wB-GcwQ/viewform) if you have any feedback or suggestions.

