package com.sumologic.elasticsearch.restlastic.dsl

trait Dsl extends DslCommons
  with IndexDsl
  with QueryDsl
  with MappingDsl
  with CompletionDsl

object Dsl extends Dsl