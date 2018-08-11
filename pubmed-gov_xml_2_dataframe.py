#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 17 17:38:22 2018

@author: Dan Wendling

Last modified: 2018-08-11

Purpose: Ingest selected XML nodes from pubmed.gov data sets, File > XML exports,
into Python dataframes.


----------------
 NOTES/WARNINGS
----------------

* Pubmed.gov data is hierarchical XML; this process removes hierarchy, arranging
the data instead as you might see in a relational database, where fields
with repeating entries (authors, grants, MeSH, publication types, etc.) 
in their own dataframes. So, use PMID as your "primary key" for pubmedarticle,
and use it as the "foreign key" in the other tables.
* Some records will be left out! Nothing is captured for <PubmedBookArticle>, 
for example. The below captures journal articles, <PubmedArticle>.
* Structured abstracts use the NLM structure labels (@NlmCategory); the less
standardized labels from the original source articles are not included.
* Some elements are simplified. More items could be added - this is not 
everything. Each section has a URL for more information.
* MeSH is thrown in here as one column, in case you want to use natural
language processing on TI-AB-MeSH, etc. The MeSH content ends with 
semi-colon space; code to prevent this is available (somewhere, not here).
* While MeSH has its own "table," it is also included as a concatenated field
in pubmedarticle - in case you want to run NLM on one dataframe.
* Not included here are repeating-entry nodes such as ArticleIdList (except
for grabbing the PubMed Central ID, if available) and History...


-------------------------
 FIXME - RECOMMENDATIONS
-------------------------

* Need one function to re-use code, to reduce chances for error.
* Need to represent nulls in the dataframes for all desired features/columns; 
currently what is brought into df is only the tags found in the current XML
file, too limiting.


----------------
SCRIPT CONTENTS
----------------

1. Start-up / What to put into place, where
2. Process/Explore pubmedarticle
3. Process/Explore author
4. Process/Explore mesh
5. Process/Explore grant
6. Process/Explore chemical
7. Process/Explore pubtype

Partly based on https://stackoverflow.com/questions/49439081/nested-xml-file-to-pandas-dataframe
"""


#%%
# ============================================
# 1. Start-up / What to put into place, where
# ============================================

import lxml.etree as et
import pandas as pd
# import os

# Set working directory
# os.chdir('/Users/name/Projects/pubmed-gov_xml_2_python_dataframe')

# localDir = 'pubmed-gov_xml_2_python_dataframe/'

'''
For your file of database records, Go to pubmed.gov, run your strategy, 
and select Send to > File > Format: XML > Create File
'''

# Set XML and XSLT to your filenames
doc = et.parse("xml/Opioids-Caregivers.xml")
xsl_pubmedarticle = et.parse("xslt/tbl_pubmedarticle.xsl")
xsl_author = et.parse("xslt/tbl_author.xsl")
xsl_grant = et.parse("xslt/tbl_grant.xsl")
xsl_chemical = et.parse("xslt/tbl_chemical.xsl")
xsl_mesh = et.parse("xslt/tbl_mesh.xsl")
xsl_pubtype = et.parse("xslt/tbl_pubtype.xsl")


#%%
# ===================================
# 2. Process/Explore pubmedarticle
# ===================================
'''
Adapt XSLT as needed - https://www.nlm.nih.gov/bsd/licensee/data_elements_doc.html
NOTE - CitationStatus is very important! There are multiple phases of 
record completeness. If a record does not have CitationStatus = MEDLINE, 
it may not be a complete record at the time you downloaded the record set. 
This means you may have basic information, but NOT grant information, or 
MeSH information, etc. Usually.
https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#status_value  
'''

# Run the transform
transformer = et.XSLT(xsl_pubmedarticle)
result = transformer(doc)

data = []
for i in result.xpath('/*'):
    inner = {}
    for j in i.xpath('*'):
        inner[j.tag] = j.text

    data.append(inner)

pubmedarticle = pd.DataFrame(data)

# Output to console
# print(result)

pubmedarticle.columns

# Select columns, column order
pubmedarticle = pubmedarticle[['PMID', 'CitationStatus', 'PMC', 'ArticleTitle', 'JournalTitle', 'Volume', 'Issue', 'Pagination', 'ArticleDateYear', 'DateCreatedYear', 'Abstract', 'MeSH']]


# Article count by journal name
articleCountByJournalName = pubmedarticle['JournalTitle'].value_counts().reset_index()
articleCountByJournalName = articleCountByJournalName.rename(columns={'JournalTitle': 'Count', 'index': 'JournalTitle'})
articleCountByJournalName.head(n=20)
print("Journal titles, Top 20 (max)\n{}".format(articleCountByJournalName))

# Citation status (records are built incrementally)
# https://www.nlm.nih.gov/bsd/mms/medlineelements.html#stat
articleStatus = pubmedarticle['CitationStatus'].value_counts().reset_index()
articleStatus = articleStatus.rename(columns={'CitationStatus': 'Count', 'index': 'CitationStatus'})
articleStatus.head()

# Count of articles in PubMed Central
print("Available in PubMed Central (free full text)\n{}".format(pubmedarticle['PMC'].notnull().sum()))


#%%
# ===================================
# 3. Process/Explore author
# ===================================
'''
Adapt XSLT as needed - https://www.nlm.nih.gov/bsd/licensee/data_elements_doc.html;
https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#authorlist.
'''

transformer = et.XSLT(xsl_author)
result = transformer(doc)

data = []
for i in result.xpath('/*'):
    inner = {}
    for j in i.xpath('*'):
        inner[j.tag] = j.text

    data.append(inner)

author = pd.DataFrame(data)


# Output to console
# print(result)

author.columns
      
'''
Reduce to what you need, reorder. Will not bring in columns that are not in the
XML; potential list is PMID, AuthorListCompleteYN, AuthorValidYN, LastName,
ForeName, Suffix, Initials, CollectiveName, EqualContrib, ConstructedPersonName,
Affiliation, IdentifierSource, Identifier
'''
# FIXME - Useful to have Affiliation, but will cause error if it wasn't in the XML
# Select columns, column order
author = author[['PMID', 'AuthorListCompleteYN', 'AuthorValidYN', 'LastName',
                 'ForeName', 'Initials', 'ConstructedPersonName', 'Affiliation']]

# Total person names
print("Total person names found: {}".format(len(author)))

# Total unique person names
print("Total unique person names: {}".format(author['ConstructedPersonName'].nunique()))

# Where are authors based?
authorCountByAfilliation = author['Affiliation'].value_counts().reset_index()
authorCountByAfilliation = authorCountByAfilliation.rename(columns={'Affiliation': 'Author count', 'index': 'Author affiliation'})
authorCountByAfilliation.head(n=20)
print("The institutions person names are based at, Top 20 (max)\n{}".format(authorCountByAfilliation))

# How many articles are associated with each institution?
# Author affiliation field is not controlled, making this messy/inaccurate!
# <Identity> tag cleans this up but is not in most records.
# FIXME - Good candidate for natural language processing.
articleCountByInstitution = author.groupby('Affiliation')['PMID'].nunique().reset_index()
articleCountByInstitution = articleCountByInstitution.rename(columns={'Affiliation': 'Author affiliation', 'PMID': 'PMID count'})
print("Clean up this dataframe manually! Article count by institution\n{}".format(articleCountByInstitution))



#%%
# ===================================
# 4. Process/Explore mesh
# ===================================
'''
Adapt XSLT as needed - https://www.nlm.nih.gov/bsd/licensee/data_elements_doc.html;
https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#meshheadinglist.
'''
    
transformer = et.XSLT(xsl_mesh)
result = transformer(doc)

data = []
for i in result.xpath('/*'):
    inner = {}
    for j in i.xpath('*'):
        inner[j.tag] = j.text

    data.append(inner)

mesh = pd.DataFrame(data)


# Output to console
# print(result)

mesh.columns

# Select columns, column order
# Choices: PMID, DescriptorName, MajorDNTopicYN, MeshUI, QualifierName,
# QualifierNameMajorTopicYN, QualifierNameUI
       
mesh = mesh[['PMID', 'DescriptorName', 'MajorDNTopicYN', 'MeshUI', 
             'QualifierName', 'QualifierNameMajorTopicYN', 'QualifierNameUI']]    

# Total MeSH entries
print("Total MeSH terms used: {}".format(len(mesh)))

# Total unique MeSH terms
print("Unique MeSH terms: {}".format(mesh['DescriptorName'].nunique()))

# Total unique Qualifier terms used
print("Unique Qualifier terms: {}".format(mesh['QualifierName'].nunique()))

# What MeSH terms are represented?
meshTermCounts = mesh['DescriptorName'].value_counts().reset_index()
meshTermCounts = meshTermCounts.rename(columns={'DescriptorName': 'Article count', 'index': 'MeSH term'})
meshTermCounts.head(n=20)
print("The MeSH terms represented, Top 20 (max)\n{}".format(meshTermCounts))

# What Qualifier names are represented?
qualifierNameCounts = mesh['QualifierName'].value_counts().reset_index()
qualifierNameCounts = qualifierNameCounts.rename(columns={'QualifierName': 'Article count', 'index': 'Qualifier name'})
qualifierNameCounts.head(n=20)
print("The Qualifier names represented, Top 20 (max)\n{}".format(qualifierNameCounts))


#%%
# ===================================
# 5. Process/Explore grant
# ===================================
'''
Adapt XSLT as needed - https://www.nlm.nih.gov/bsd/licensee/data_elements_doc.html;
    https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#grantlist;
    https://www.nlm.nih.gov/bsd/grant_acronym.html.
'''

transformer = et.XSLT(xsl_grant)
result = transformer(doc)

data = []
for i in result.xpath('/*'):
    inner = {}
    for j in i.xpath('*'):
        inner[j.tag] = j.text

    data.append(inner)

grant = pd.DataFrame(data)


# Output to console
# print(result)

grant.columns
      
'''
Reduce to what you need, reorder. Will not bring in columns that are not in the
XML; potential list is PMID, Agency, Acronym, Country, GrantID, GrantListCompleteYN
'''
# Select columns, column order
grant = grant[['PMID', 'GrantListCompleteYN', 'GrantID', 'Agency', 'Acronym', 'Country']]

# Total grant numbers
print("Total grant numbers found: {}".format(len(grant)))

# Total unique grant numbers
print("Total unique grant numbers: {}".format(grant['GrantID'].nunique()))

# What agencies are represented?
grantCountByAgency = grant['Agency'].value_counts().reset_index()
grantCountByAgency = grantCountByAgency.rename(columns={'Affiliation': 'grant count', 'index': 'grant affiliation'})
grantCountByAgency.head(n=20)
print("The agencies represented, Top 20 (max)\n{}".format(grantCountByAgency))


#%%
# ===================================
# 6. Process/Explore chemical
# ===================================
'''
Adapt XSLT as needed - https://www.nlm.nih.gov/bsd/licensee/data_elements_doc.html;
    https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#chemicallist.
'''

transformer = et.XSLT(xsl_chemical)
result = transformer(doc)

data = []
for i in result.xpath('/*'):
    inner = {}
    for j in i.xpath('*'):
        inner[j.tag] = j.text

    data.append(inner)

chemical = pd.DataFrame(data)


# Output to console
# print(result)

chemical.columns
      
# Select columns, column order
# Choices: PMID, RegistryNumber, UI, 
chemical = chemical[['PMID', 'RegistryNumber', 'UI', 'NameOfSubstance']]

# Total substance entries
print("Total chemical entries: {}".format(len(chemical)))

# Total unique chemicals
print("Total unique chemicals: {}".format(chemical['UI'].nunique()))

# What chemicals are represented?
chemicalCounts = chemical['NameOfSubstance'].value_counts().reset_index()
chemicalCounts = chemicalCounts.rename(columns={'NameOfSubstance': 'Article count', 'index': 'NameOfSubstance'})
chemicalCounts.head(n=20)
print("The chemicals represented, Top 20 (max)\n{}".format(chemicalCounts))

#%%
# ===================================
# 7. Process/Explore pubtype
# ===================================
'''
Adapt XSLT as needed - https://www.nlm.nih.gov/bsd/licensee/data_elements_doc.html;
https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#publicationtypelist;
https://www.ncbi.nlm.nih.gov/books/NBK3827/table/pubmedhelp.T.publication_types/?report=objectonly.
'''

transformer = et.XSLT(xsl_pubtype)
result = transformer(doc)

data = []
for i in result.xpath('/*'):
    inner = {}
    for j in i.xpath('*'):
        inner[j.tag] = j.text

    data.append(inner)

pubtype = pd.DataFrame(data)


# Output to console
# print(result)

pubtype.columns

# Select columns, column order
# Choices: PMID, PublicationType, PublicationTypeUI
pubtype = pubtype[['PMID', 'PublicationType']]

# Total pubtype entries
print("Total pubtype entries found: {}".format(len(pubtype)))

# Total unique pubtypes
print("Total unique pubtype entries: {}".format(pubtype['PublicationType'].nunique()))

# What pubtypes are represented?
pubTypeCount = pubtype['PublicationType'].value_counts().reset_index()
pubTypeCount = pubTypeCount.rename(columns={'Affiliation': 'grant count', 'index': 'grant affiliation'})
pubTypeCount.head(n=20)
print("The pubtypes represented, Top 20 (max)\n{}".format(pubTypeCount))
