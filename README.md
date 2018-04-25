F3CP
====

F3CP is a simple object export/ingest utility.
It will copy a list of object from [Fedora 3](https://wiki.duraspace.org/display/FEDORA38/Fedora+3.8+Documentation) into a JSON file.
It will upload a list of objects described in a JSON file (in a suitable format) into a Fedora 3 instance.

(Why not use the Fedora 3 import/export interface? Don't ask questions!)

# Usage

To copy a few objects into a JSON file:

    f3cp dump https://user:pass@host/fedora  temp:123 temp:124 temp:125 > out.json

To copy the items from a JSON file into fedora:

    f3cp load https://user:pass@host/fedora < out.json

The previous will stream the objects, so only one object is held in memory at a time.


# Format of JSON File

The file is an array of objects.
Each JSON object represents a single Fedora object.
The object has metadata fields for the PID, the label, the dates created and
modified, and the state of the object.
It also has a list giving information for each datastream, as well as the contents
of the datastream.
If the contents are not valid utf-8, then the they are base64 encoded.
(That is why there is both a `Content` and `ContentBase64` keys.
If both are provided, the `Content` key is used).

```
{
  "PID": "und:ks65h990v3w",
  "Label": "",
  "CreateDate": "2014-07-05T16:58:15.636Z",
  "LastModDate": "0001-01-01T00:00:00Z",
  "State": "A",
  "DSitems": [
    {
      "Name": "DC",
      "Label": "Dublin Core Record for this object",
      "VersionID": "DC1.0",
      "State": "A",
      "Checksum": "",
      "ChecksumType": "DISABLED",
      "MIMEType": "text/xml",
      "Location": "und:ks65h990v3w+DC+DC1.0",
      "LocationType": "",
      "ControlGroup": "X",
      "Versionable": true,
      "Size": 344,
      "Content": "\n<oai_dc:dc xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd\">\n  <dc:identifier>und:ks65h990v3w</dc:identifier>\n</oai_dc:dc>\n",
      "ContentBase64": null
    },
    {
      "Name": "content",
      "Label": "ephemera_cuala_5002.xml",
      "VersionID": "content.0",
      "State": "A",
      "Checksum": "",
      "ChecksumType": "DISABLED",
      "MIMEType": "application/xml",
      "Location": "und:ks65h990v47+content+content.0",
      "LocationType": "INTERNAL_ID",
      "ControlGroup": "M",
      "Versionable": true,
      "Size": 132118,
      "Content": "",
      "ContentBase64": "PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iaXNv...ZXNjPgogPC9lYWQ+Cg=="
    }
  ]
}
```
