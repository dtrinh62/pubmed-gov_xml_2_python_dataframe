# pubmed-gov_xml_2_python_dataframe

> A way to move pubmed.gov records into Pandas dataframes for further processing.

Use this XSLT file and Python script when you want to work with pubmed.gov exports in Python. I am using 20,000 records or less.

At pubmed.gov: 

- Run your search strategy
- Send to > File > Format: XML
- Update file name in script

More info inside py file.

![screensot](pm_dataframe.png)

Relies on tag information at https://www.nlm.nih.gov/bsd/licensee/data_elements_doc.html. Requires pandas and lxml packages. Authored in Spyder editor.

Serving suggestion - video about a ColdFusion app that is out of date; I hope to move it to Python-Flask or Python-Django:

> http://www.screencast.com/t/Yr7hdMxA 
> If asked, enter the following password: NewHLRev
