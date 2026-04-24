
# Feature Engineering
The main part of the analysis was RFM (Recency, Frequency, Monetary). and then
we build on top of this. even though I have followed the instructions I want to write why I object to some parts.

## Why I think RFM analysis is a very bad analysis.
We are mixing some irrelevant data together. (Purchase Information and Time Variable.) Mixing variables of many different type in general is a very bad idea, at some point it can be very inactionable. I would rather build many clusters in parallel with different types of data. ex. demographic, purchase_cluster, dormancy-cluster, product-category-cluster. 


# What I've built
The results can be found in src/features/feature_manifest.json
- rfm features
- order features
- return features
  
# What I suggest instead
- We remove rfm and instead add dormancy.
- The Recency and Frequency convert to Dormancy.
**Dormancy**: We use a Poisson distribution to find out when a customer is going to make a new order. When the probability goes really high but the customer hasn't make the order yet, we say the customer is dormant. At some point we can also say the customer has churned. This only applies for customers with more than 2.
- We can add a set of data with demographic information. (Age, Location, channel, etc.)

The project includes 


# CI/CD
The CI pipeline doesn't run because there is no data in the data folder. since the repository is public I decided not to upload the data into github.

## What is missing in the pipeline:
- testing: We can add a test for our utils, and pipeline stages.
- The project can be done with mlflow with a docker as env
- Demographic features from customers dataset.
