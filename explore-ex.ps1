$EsHost = "localhost:9200"
$EsAddress = "http://$EsHost"

# info
Invoke-WebRequest -Method GET $EsAddress

# create index
Invoke-WebRequest -Method PUT "$EsAddress/trec"

Invoke-WebRequest -Method PUT "$EsAddress/trec" -ContentType 'application/json' -Body (@{
    "settings" = @{
        "number_of_shards" = 1
        "similarity" = @{
            "default" = @{
                    "type" = "BM25"
                    "b" = 0 
                    "k1" = 0.9
            }
        }
      }
      
} | ConvertTo-Json -Depth 25) 

# add documents
Invoke-WebRequest -Method POST "$EsAddress/trec/_doc" -ContentType 'application/json' -Body (@{
    'title' = 'Covid-19 study'
    'content' = 'some flu text'
} | ConvertTo-Json -Depth 25)

# get all
Invoke-WebRequest -Method POST "$EsAddress/trec/_search" -ContentType 'application/json' -Body (@{
    'query' = @{ "match_all" = @{} }
} | ConvertTo-Json -Depth 25)

# get query
(Invoke-WebRequest -Method POST "$EsAddress/trec/_search" -ContentType 'application/json' -Body (@{
    'query' = @{
        "multi_match" = @{
            "query" = "What is flu?"
            "fields" = @("title", "content")
        }
    }
} | ConvertTo-Json -Depth 25)).Content | python -m json.tool