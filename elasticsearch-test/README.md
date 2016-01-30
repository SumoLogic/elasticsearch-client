# elasticsearch-test

### When to use this module?
Use this module if you need to write tests that depend on running a local elasticsearch instance

### What belongs in this module?
Code to spin up a local elasticsearch node and helper code to write UTs that need elasticsearch. This module was split
out so that the dependency on Elasticsearch proper could be restricted to `<scope>test</scope>`


### Who knows this module?
* Russell Cohen
