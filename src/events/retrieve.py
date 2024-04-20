
filters=[
    {
        "filterByRunOn":"openagents\\/document-retrieval"
    }
]

meta = {
    "kind": 5003,
    "name": "PDF/HTML/Plaintext URLs to Markdown",
    "about": "This action fetches content from HTML, PDF, and plaintext URLs and returns the content in markdown format.",
    "tos": "",
    "privacy": "",
    "author": "",
    "web": "",
    "picture": "",
    "tags": ["tool", "retrieval-pdf", "retrieval-html", "retrieval-plaintext"],
}

sockets={
    "in": {       
        "urls":{
            "type": "object",
            "description": "Direct URLs to PDF, HTML, or plaintext content",
            "name": "URLs",
            "schema": [
                {
                    "type": "string",
                    "description": "The URL to fetch content from",
                    "name": "URL"
                }               
            ]
        }      
    },
    "out": {
        "outputType": {
            "type": "string",
            "value": "application/json",
            "description": "The Desired Output Type",
            "name": "Output Type"
        }
    }
}


template = """{
    "kind": {{meta.kind}},
    "created_at": {{sys.timestamp_seconds}},
    "tags": [
        ["param","run-on", "openagents/document-retrieval"],
        {{#in.urls}}
        ["i", "{{.}}", "text", "",  ""],
        {{/in.urls}}     
        ["expiration", "{{sys.expiration_timestamp_seconds}}"],
    ],
    "content":""
}
"""

