## Breaking changes in 2.0.0

### _timestamp field is deprecated
_timestamp field is deprecated in elasticsearch 2.0 https://www.elastic.co/guide/en/elasticsearch/reference/2.0/mapping-timestamp-field.html

### search_type=scan and search_type=count are deprecated
search_type=scan and search_type=count are deprecated in elasticsearch 2.1 https://www.elastic.co/guide/en/elasticsearch/reference/2.1/breaking_21_search_changes.html

### QueryRoot api 
QueryRoot api has changed to 
```
QueryRoot(query: Query,
    fromOpt: Option[Int] = None,
    sizeOpt: Option[Int] = None,
    sortOpt: Option[Seq[Sort]] = None,
    timeoutOpt: Option[Int] = None,
    sourceFilterOpt: Option[Seq[String]] = None,
    terminateAfterOpt: Option[Int] = None)
```

### ElasticJsonDocument api
ElasticJsonDocument api has changed to
```ElasticJsonDocument(_index: String,
    type: String,
    _id: String,
    _score: Option[Float],
    _source: JObject,
    highlight: Option[JObject])
```
