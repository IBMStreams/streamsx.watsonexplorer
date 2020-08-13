---
title: "Toolkit Usage Overview"
permalink: /docs/user/overview/
excerpt: "How to use this toolkit."
last_modified_at: 2020-08-13T16:37:48-04:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "userdocs"
---
{% include toc %}
{%include editme %}


The new Watson Explorer toolkit allows access to documents indexed by Watson Explorer from Streams.
This means you can now correlate real-time data in Streams with at-rest data stored in Watson Explorer!

## Introduction

IBM Watson Explorer is a platform for development of enterprise information navigation and search applications. It provides users with a 360-degree view of their information while also being able to gain deeper insight via advanced text analysis.
By combining IBM Streams with Watson Explorer, users can easily correlate real-time data with historical documents indexed by Watson Explorer in order to provide context and meaning to their data. For example, combining IBM Streams with Watson Explorer can be useful in a call centre scenario. Customers may call in with specific problems or concerns related to their services. IBM Streams can perform real-time speech-to-text recognition of the conversation. Keywords extracted from the conversation between the customer and the call centre agent can be used to quickly generate a search query and retrieve relevant knowledge base articles and scripts from a Watson Explorer collection. By recommending relevant documents in real-time and in context, IBM Streams allows the agents to more efficiently address the customer?s specific concerns, resulting in faster resolution of the problem and higher customer satisfaction.
IBM [Watson Explorer](https://www.ibm.com/cloud/watson-discovery) is comprised of two sets of components: foundational components and analytics component.


## Foundation Components

The foundational components provide federated discovery, navigation, and search over a broad range of data sources and types. The software includes a framework for developing information-rich applications that deliver a comprehensive, contextually-relevant, 360-degree view of any topic for business users, data scientists, and a variety of targeted business functions.

## Analytical Components

The analytical components, which are provided in Watson Explorer Advanced Edition, help analysts transform information into insights through visualizations that reveal trends, patterns, anomalies, and relationships. The analytical components also enable custom and domain-specific text analytics, which can enhance 360-degree view and content mining applications.

(ref: [http://www.ibm.com/support/knowledgecenter/SS8NLW/SS8NLW_welcome.html](http://www.ibm.com/support/knowledgecenter/SS8NLW/SS8NLW_welcome.html))


## Watson Explorer Toolkit for Streams

The Watson Explorer toolkit enables IBM Streams applications to integrate with the Watson Explorer platform. The initial version of the toolkit is comprised of 6 operators to allow Streams applications to communicate with Watson Explorer applications that are built using either the foundational components or the analytics components. Each of the operators uses the Watson Explorer REST APIs as the communication mechanism. The initial set of operators available in the toolkit are:

### Foundational component operators:

* WatsonExplorerPush
* WatsonExplorerQueryWEX

### Analytical component operators:

* Search
* SearchFacet
* SearchPreview
* AnalyzeText

A description for each of the operators is provided in the following section.

### Operators

#### WatsonExplorerPush

This operator is intended for use in applications written using the Watson Explorer foundational components. This operator is capable of pushing text to a specific collection. The operator uses the search-collection-enqueue REST API provided by the Watson Explorer foundational components in order to push data to the collection.
More information about the available parameters for this API function can be found in the Watson Explorer Knowledge Center here.

#### WatsonExplorerQuery

This operator is intended for use in applications written using the Watson Explorer foundational components. This operator is capable of querying for documents from a specific collection. The operator uses the query-search REST API provided by the Watson Explorer foundational components in order to query the collection.
More information about the available parameters for this API function can be found in the Watson Explorer Knowledge Center here.

#### Search

This operator is intended for use in applications written using the Watson Explorer analytical components (Content Analytics). This operator is capable of searching for documents that satisfy a query. The results returned from the REST API contain a list of documents that satisfy the query, a short summary of the document text, links to the full document text and other metric information.
More information about the available parameters for this REST API can found in the Content Analytics REST API documentation in the Content Analytics installation directory: /docs/api/rest/search.

#### SearchFacet

This operator is intended for use in applications written using the Watson Explorer analytical components (Content Analytics). This operator is capable of searching for documents that satisfy a query and returning only the facets for those documents. The query can be further narrowed by specifying which facets should be searched via the facet or facetAttr parameters. The results returned from the REST API contain the metrics for each of the facets returned as well as some additional metrics.
More information about the available parameters for this REST API can found in the Content Analytics REST API documentation in the Content Analytics installation directory: /docs/api/rest/search/facet.

#### SearchPreview

This operator is intended for use in applications written using the Watson Explorer analytical components (Content Analytics). This operator is capable of returning the entire document text for a specific document. The document whose text should be returned can be specified using the uriAttr parameter. The value for this parameter is contained in the result set returned by the CASearch operator.
More information about the available parameters for this REST API can found in the Content Analytics REST API documentation in the Content Analytics installation directory: /docs/api/rest/search.

#### AnalyzeText

This operator is intended for use in applications written using the Watson Explorer analytical components (Content Analytics). This operator is capable of perform real-time natural language processing (NLP) against text data. The operator can ingest a string, send the text to a specific collection for analysis and return the result set. The analysis that is run against the textual data depends on the annotators that are configured for the specified collection. The result set contains facet data.
More information about the available parameters for this REST API can found in the Content Analytics REST API documentation in the Content Analytics installation directory: /docs/api/rest/search.


### Operator setup

#### Foundational component operators

The **WatsonExplorerPush** and **WatsonExplorerQuery** operators contain the following common set of parameters:

Parameter name	| Description
-------- | -------- 
host	| The host name of the REST server
port	| The port number of the REST server
username	| The username to use when connecting to the REST server
password	| The password to use when connecting to the REST server
resultAttrName	| The name of the output attribute that should hold the results (default is ?result?)
additionalParams	| Allows for specifying a list of additional parameters to include with the REST API call. See the next section for details on this parameter.

Each operator also contains parameters that are specific to the operation of that operator. More information about the available parameters can be found in the SPLDoc for the toolkit.


#### Analytical components operators

The **Search**, **SearchFacet**, **SearchPreview** and **AnalyzeText** contain the following common set of parameters:

Parameter name	| Description
-------- | -------- 
host	| The host name of the REST server
port	| The port number of the REST server
username	| The username to use when connecting to the REST server
password	| The password to use when connecting to the REST server
collectionName	| The name of the collection. Use either collectionName or collectionNameAttr. Can?t specify both.
collectionNameAttr	| The input attribute that contains the collection name
outputFormat	| The format of the result set (default is XML)
resultAttrName	| The name of the output attribute that should hold the results (default is ?result?)
additionalParams	| Allows for specifying a list of additional parameters to include with the REST API call. See the next section for details on this parameter.

For more information about the available parameters in each of the operators, see the [SPLDoc](https://ibmstreams.github.io/streamsx.watsonexplorer/spldoc/html/index.html) for the toolkit.


#### Using the additonalParams parameter

The operators use the REST API provided by the Watson Explorer foundational components in order to push and query data. The underlying REST APIs that the operators use contain dozens of possible parameters for configuring the underlying REST API calls. Surfacing each of these REST API parameters as operator parameters would not only have made the operator bloated and confusing to use, but it would have made maintaining the operator a major challenge. Instead, additional REST API parameters can be specified using the `additionalParams` operator parameter. This operator parameter is capable of accepting a list of key/value pairs that are parsed and passed to the underlying REST API call. Here is an example of how to specify additional REST API parameters using the `additionalParams` operator parameter:

```
(stream WEXQueryStream) as WEXQueryOp = WatsonExplorerQuery(input) {
param
additionalParams : ?key1=value1?, ?key2=value2?, ?key3=value3?
?
}
```


### streamsx.watsonexplorer toolkit

This toolkit has been developed and tested against Watson Explorer v11.0.

[SPLDoc for the com.ibm.streamsx.watsonexplorer toolkit](https://ibmstreams.github.io/streamsx.watsonexplorer/spldoc/html/index.html)

#### Samples

Samples demonstrating how to use each of the operators can be found in the samples directory in the [streamsx.watsonexplorer](https://github.com/IBMStreams/streamsx.watsonexplorer) repository.


