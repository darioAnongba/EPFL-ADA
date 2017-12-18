# Is adopting a healthier lifestyle a long-term social change?

# Abstract
> *"A healthy lifestyle, excluding any damaging influences, defines the positive and voluntary measures a person can implement to maintain good mental and physical health. This includes healthy habits in terms of diet, treatment of the body, sex, and the environment"*, http://health.ccm.net 

The idea of the project is to assess whether the population of western countries is adopting a healthier lifestyle. This project aims to determine if this adoption is a trend or a long-term social change. In either cases, we will try to predict the evolution of this behavior over the upcoming years.

In order to achieve that goal, we will use the Amazon reviews dataset. From those reviews, we can extract information about consumer satisfaction, dates, product categories and the enthousiasm for certain products.

With the emergence of new institutions promoting healthy lifes (like vegan shops or fitness centers), finding insights and patterns in people's behavior could be useful to define in which direction this social change is heading.

# Research questions
* Study the evolution of the enthusiasm for healthy products over time.
* Determine the "hype" factor of healthy product categories.
* Study the hype for healthy products in comparison with general products of the same categories
* Identify the evolution of the hype for healthy product over time. 

# Dataset
* Amazon reviews (Sports and Outdoors, Grocery and Gourmet Food)

Here is an example of how the "vegan" keyword is an actual trend and the predictions for that keyword in the future (by Google analytics) from 2012 to 2017:

![Interest evolution for the "vegan" keyword](vegan_trend.png)

##### Amazon reviews
First of all, we will load the data into Pandas dataframes. The data being in non-strict JSON format, we will have to parse it into strict JSON.

Then, we will process the data to filter out all non-healthy related reviews and products (because of the huge size of the dataset).

Finally, in order to determine if a product or a category of products is becoming more or less popular, we will compute several metrics and plot them in order to find correlations between other products that either have been decaying in reviews (and thus sales) or on contrary, that have been growing in popularity.

## Plan for milestone 2
|Week|Expected task|
|---|---|
|Week 1| Setup the raw data by downloading and reading the data into pandas dataframes.
|Week 2| Filter the data to keep only healthy products. Also compute the growth of Amazon in general to avoid biases in the results.
|Week 3 - 4| Write the report showing the data collection pipeline and the descriptive analysis.

## Plan for milestone 3

|Week|Expected task|
|---|---|
|Week 1 and 2| Computing and plotting all the metrics defined in the mathematicals details section of our report.
|Week 3| Writing the report while analyzing the results and trying to come up with responses to the initial research questions.

## Workload 
The workload was equally distributed among group members. We met twice every week to distribute the work and all members participated in all aspects of the project, from data preprocessing, to the report.
