## As part of a course introduction to big data
- Loads imdb data to sql table via JDBC in accordance with a given schema (IMDBLoader)
- Creates views from imdb data based on a given query (GenerateMappings)
- Performs data cleaning/integration after creating new sources according to given query (createsources & FillFromWikidata)
- Performs Association mining using apriori in numerous steps to generate Lk and Ck after initializing the data ( Apriori)
- Performs Kmeans clustering after initializing the data, performing KMeans and comparing the clusters using silhouette coefficient and mutual information(2 separate metrics to internal and external evaluation) stored in KMeansClustering folder

