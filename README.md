# BigData
## Authors
Dan Cristian Cecoi 2193860C
Naji Shehab 2205596S
Sridha Agarwal 2208414A


## Solution
We have used three Mappers and two Reducers for this exercise
### Job 1: Parsing Input Data
#### Mapper
This reads in the file line by line and emits the article title and one outlink per key
file -> **<article_title, outlink>**
#### Reducer
This iterates over the key-value pairs and emits the article_title, pagerank score (initially set to 1.0) and the list of all outlinks. It also checks for duplicating outlinks and self-loops
<article\_title, outlink> -> **<article_title, PR, [list of outlinks]>**

### Job 2: Calculating Pagerank
This job runs iteratively calculating the ranking, the higher the number of iterations, the more accurate the ranking
#### Mapper
This takes in the output from the previous reducer as its input <article_title, PR, [list of outlinks]> and emits two key value pairs:
i) iterates over all outlinks of current article and emits **<outlink, PR/no. of outlinks>**
ii) and finally, it emits the article title and list of all its outlinks **<article_title, [list of outlinks]>**
#### Reducer
This adds all the pagerank from the <outlink, PR/no. of outlinks> pair and updates the articles pagerank emitting
**<article_title, PR, [list of outlinks]>**

### Job 3: Displaying Pagerank
This takes in the output from the previous reducer and emits the pagerank for each article
**<article_title, PR>**



