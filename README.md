# Disclaimer
This is a fork of AQP++ implementation originally from jinglin (jinglin_peng AT sfu DOT ca). Below is the content from the repo and more detail are present in here: <a href="https://github.com/sfu-db/aqppp/tree/master" target="_blank">GitHub</a> 

# Data
The framework needs a specific skewed TPCH dataset for successful execution. However, for debugging we generate the standard lineitem table for windows.
TPCH can be downloaded here: https://www.tpc.org/TPC_Documents_Current_Versions/download_programs/tools-download-request5.asp?bm_type=TPC-H&bm_vers=3.0.1&mode=CURRENT-ONLY

To use TPCH in windows: https://tedamoh.com/en/blog/data-modeling/78-generating-large-example-data-with-tpc-h

Finally, dbgen is executed with options -T L & -s 1 for generating lineitem table for 1GB data size (make sure you have enough space for the data :-) )

# About
This is the repo for paper 'AQP++: Connecting Approximate Query Processing With Aggregate Precomputation for Interactive Analytics'. The main idea of this work is using precomputed query results to improve the query quality of AQP (sampling) method. Our results show that AQP++ could trade only a little additional overhead to get a much better answer quality (e.g. 10X more accurate than AQP method!)  

Currently the repo contains the experimental code. We will release version 1.0 very soon.

The experimental code was written using Visual Studio 2017, and built on top of SQL Server 2017. It could run on Windows system.  

# Link
<a href="../master/SIGMOD2018_AQP%2B%2B_paper.pdf" target="_blank">paper</a>

<a href="../master/SIGMOD2018_AQP%2B%2B_slides.pdf" target="_blank">slides</a>

# Questions?
If you have any question, please feel free to connect jinglin (jinglin_peng AT sfu DOT ca).
