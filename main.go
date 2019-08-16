package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/knakk/rdf"
)

type FObject struct {
	ObjectInfo
	DSitems []DSentry
}

type DSentry struct {
	DsInfo
	Content       string
	ContentBase64 []byte
}

func main() {
	if len(os.Args) < 3 {
		usage()
		return
	}
	remote := NewRemote(os.Args[2])
	switch os.Args[1] {
	case "dump":
		DumpList(remote, os.Stdout, os.Args[3:])
	case "load":
		LoadList(remote, os.Stdin)
	case "item":
		DownloadCurateObjects(remote, os.Args[3:])
	default:
		fmt.Fprintf(os.Stderr, "Unknown command %s", os.Args[1])
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `USAGE:
	f3cp dump <remote fedora> <pid1> [<pid2> ...]

Dump a JSON representation of the pids given to STDOUT. This will include
all current datastream contents.

	f3cp load <remote fedora>

Copy the JSON provided on STDIN into the given fedora, possibly overwriting
any existing objects.

You should include a username and password if your instance of fedora requires
it. e.g. https://username:password@host/fedora

The dump and load only keep one object in memory at a time.
`)
}

// DumpList dumps a list of ids from the given fedora instance to out as
// an well-formed JSON array. The objects are output in the order given
// in the id array. Only one object is kept in memory at a time, so this
// can handle a long list of objects. Status updates and errors are printed to
// STDERR.
func DumpList(remote *remoteFedora, out io.Writer, ids []string) {
	enc := json.NewEncoder(out)
	// we manually format the enclosing list part of the JSON. Each object in
	// the list is serialized using the encoder.
	fmt.Fprintf(out, "[")
	first := true
	for _, id := range ids {
		if !first {
			fmt.Fprintf(out, ",")
		}
		first = false
		fmt.Fprintln(os.Stderr, "dumping", id)
		obj, err := FetchOneObject(remote, id)
		if err != nil {
			fmt.Fprintf(os.Stderr, id, err)
			continue
		}
		enc.Encode(obj)
	}
	fmt.Fprintf(out, "]")
}

// FetchOneObject loads every datastream from the fedora object id from remote
// and returns it as an FObject. It loads all of the datastreams in memory, so
// there is a potential for extremely large objects to run out of memory.
func FetchOneObject(remote *remoteFedora, id string) (*FObject, error) {
	var err error
	result := FObject{}
	result.ObjectInfo, err = remote.GetObjectInfo(id)
	if err != nil {
		return nil, err
	}
	dsNames, err := remote.GetDatastreamList(id)
	if err != nil {
		return nil, err
	}
	// load the datastreams in alphabetical order
	sort.StringSlice(dsNames).Sort()
	for _, ds := range dsNames {
		var entry DSentry
		entry.DsInfo, err = remote.GetDatastreamInfo(id, ds)
		if err != nil {
			return nil, err
		}
		if entry.Size > 0 {
			body, err := remote.GetDatastream(id, ds)
			if err != nil {
				return nil, err
			}
			entry.ContentBase64, err = ioutil.ReadAll(body)
			body.Close()
			if err != nil {
				return nil, err
			}
			if utf8.Valid(entry.ContentBase64) {
				entry.Content = string(entry.ContentBase64)
				entry.ContentBase64 = nil
			}
		}
		result.DSitems = append(result.DSitems, entry)
	}
	return &result, nil
}

func LoadList(remote *remoteFedora, source io.Reader) error {
	// read objects from json list one at a time
	dec := json.NewDecoder(source)

	// read open bracket
	_, err := dec.Token()
	if err != nil {
		return err
	}

	// while the array contains values
	for dec.More() {
		var obj FObject
		// decode an array value
		err := dec.Decode(&obj)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return err
		}

		fmt.Fprintln(os.Stderr, "loading", obj.PID)
		err = UploadOneObject(remote, obj)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return err
		}
	}

	// read closing bracket
	_, err = dec.Token()
	return err
}

func UploadOneObject(remote *remoteFedora, obj FObject) error {
	// does object exist?
	_, err := remote.GetObjectInfo(obj.PID)
	if err == ErrNotFound {
		err = remote.MakeObject(obj.ObjectInfo)
	}
	if err != nil {
		return err
	}
	// now upload each datastream
	for _, ds := range obj.DSitems {
		// skip fedora special datastreams
		if ds.Name == "DC" {
			continue
		}
		// choose the correct source for this datastream content
		// n.b. it is possible that source will remain nil
		// that means there is no content to upload.
		var source io.Reader
		if ds.Content != "" {
			source = strings.NewReader(ds.Content)
		} else if len(ds.ContentBase64) > 0 {
			source = bytes.NewReader(ds.ContentBase64)
		}
		_, err = remote.GetDatastreamInfo(obj.PID, ds.Name)
		if err == ErrNotFound {
			err = remote.MakeDatastream(obj.PID, ds.DsInfo, source)
		} else if err == nil {
			err = remote.UpdateDatastream(obj.PID, ds.DsInfo, source)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

type Triples struct {
	Subject   string
	Predicate string
	Object    string
}

type CurateItem struct {
	PID  string
	Meta []Triples
}

func (c *CurateItem) Add(predicate string, value string) {
	if value == "" {
		return
	}
	c.Meta = append(c.Meta, Triples{
		Subject:   c.PID,
		Predicate: predicate,
		Object:    value,
	})
}

func (c *CurateItem) Add3(subject string, predicate string, value string) {
	if value == "" {
		return
	}
	c.Meta = append(c.Meta, Triples{
		Subject:   subject,
		Predicate: predicate,
		Object:    value,
	})
}

// FetchOneCurateObject loads the given fedora object and interpretes it as if
// it were a curate object. This means only certain datastreams are downloaded.
func FetchOneCurateObject(remote *remoteFedora, id string) (*CurateItem, error) {
	var err error
	result := &CurateItem{PID: id}
	// Assume the id is a curate object, which means we know exactly which
	// datastreams to look at
	err = ReadRelsExt(remote, id, result)
	if err != nil {
		fmt.Fprintln(os.Stderr, id, err)
	}
	err = ReadProperties(remote, id, result)
	if err != nil {
		fmt.Fprintln(os.Stderr, id, err)
	}
	err = ReadRightsMetadata(remote, id, result)
	if err != nil {
		fmt.Fprintln(os.Stderr, id, err)
	}
	err = ReadDescMetadata(remote, id, result)
	if err != nil {
		fmt.Fprintln(os.Stderr, id, err)
	}
	err = ReadContent(remote, id, result)
	// only GenericFiles have content and thumbnail datastreams
	if err != nil && err != ErrNotFound {
		fmt.Fprintln(os.Stderr, id, err)
	}
	err = ReadThumbnail(remote, id, result)
	if err != nil && err != ErrNotFound {
		fmt.Fprintln(os.Stderr, id, err)
	}
	err = ReadBendoItem(remote, id, result)
	if err != nil && err != ErrNotFound {
		fmt.Fprintln(os.Stderr, id, err)
	}
	return result, nil
}

func DownloadCurateObjects(remote *remoteFedora, ids []string) error {
	w := csv.NewWriter(os.Stdout)
	w.Comma = '\t'

	for _, id := range ids {
		fmt.Fprintln(os.Stderr, "Fetching", id)
		v, err := FetchOneCurateObject(remote, id)
		if err != nil {
			fmt.Fprintln(os.Stderr, id, err)
		}

		for _, t := range v.Meta {
			line := []string{t.Subject,
				t.Predicate,
				strings.ReplaceAll(t.Object, "\n", "\\n"),
			}
			err = w.Write(line)
			if err != nil {
				fmt.Fprintln(os.Stderr, id, err)
			}
		}
	}

	w.Flush()
	if err := w.Error(); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	return nil
}

type PredicatePair struct {
	XMLName xml.Name
	V       string `xml:",any,attr"`
}
type RelsExtDS struct {
	Description struct {
		P []PredicatePair `xml:",any"`
	} `xml:"Description"`
}

func ReadRelsExt(remote *remoteFedora, id string, result *CurateItem) error {
	body, err := remote.GetDatastream(id, "RELS-EXT")
	if err != nil {
		return err
	}
	defer body.Close()
	// using the rdf decoder with rdf.RDFXML caused problems since the decoder
	// thought the `info:` scheme used by fedora was an undeclared namespace.
	// so we decode it ourself. The XML/RDF used by fedora in RELS-EXT is
	// very limited and structured, so this is not expected to be a problem.
	// (e.g. every tuple must have the given resource as a subject).
	var v RelsExtDS
	dec := xml.NewDecoder(body)
	err = dec.Decode(&v)
	body.Close()

	for _, p := range v.Description.P {
		// this isn't taking namespace into account...
		p.V = ApplyPrefixes(p.V)
		switch p.XMLName.Local {
		case "hasModel":
			result.Add("af-model", p.V)
		case "isMemberOfCollection":
			result.Add("isMemberOfCollection", p.V)
		case "isPartOf":
			result.Add("isPartOf", p.V)

		// make sure the permission labels match those used in rightsMetadata
		case "hasEditor":
			result.Add("edit-person", p.V)
		case "hasEditorGroup":
			result.Add("edit-group", p.V)

		default:
			result.Add(ApplyPrefixes(p.XMLName.Space+p.XMLName.Local), p.V)
		}
	}

	return nil
}

var Prefixes = map[string]string{
	"info:fedora/und:":                                       "und:",
	"info:fedora/afmodel:":                                   "",
	"http://purl.org/dc/terms/":                              "dc:",
	"https://library.nd.edu/ns/terms/":                       "nd:",
	"http://purl.org/ontology/bibo/":                         "bibo:",
	"http://www.ndltd.org/standards/metadata/etdms/1.1/":     "ms:",
	"http://purl.org/vra/":                                   "vracore:",
	"http://id.loc.gov/vocabulary/relators/":                 "mrel:",
	"http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#": "ebucore:",
	"http://xmlns.com/foaf/0.1/":                             "foaf:",
	"http://projecthydra.org/ns/relations#":                  "hydra:",
	"http://www.w3.org/2000/01/rdf-schema#":                  "rdfs:",
	"http://purl.org/pav/":                                   "pav:",
}

func ApplyPrefixes(s string) string {
	for k, v := range Prefixes {
		if strings.HasPrefix(s, k) {
			return v + strings.TrimPrefix(s, k)
		}
	}
	return s
}

func ReadDescMetadata(remote *remoteFedora, id string, result *CurateItem) error {
	body, err := remote.GetDatastream(id, "descMetadata")
	if err != nil {
		return err
	}
	defer body.Close()
	triples := rdf.NewTripleDecoder(body, rdf.NTriples)
	for {
		v, err := triples.Decode()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		subject := ApplyPrefixes(v.Subj.String())
		if subject != id {
			subject = id + "/" + subject
		}
		result.Add3(subject,
			ApplyPrefixes(v.Pred.String()),
			ApplyPrefixes(v.Obj.String()))
	}

	return nil
}

type Access struct {
	Type    string   `xml:"type,attr"`
	Persons []string `xml:"machine>person"`
	Groups  []string `xml:"machine>group"`
}
type rightsDS struct {
	Access  []Access `xml:"access"`
	Embargo string   `xml:"embargo>machine"`
}

func ReadRightsMetadata(remote *remoteFedora, id string, result *CurateItem) error {
	body, err := remote.GetDatastream(id, "rightsMetadata")
	if err != nil {
		return err
	}
	var v rightsDS
	dec := xml.NewDecoder(body)
	err = dec.Decode(&v)
	body.Close()
	if err != nil {
		return err
	}

	result.Add("embargo-date", v.Embargo)
	for _, access := range v.Access {
		var grouplabel string
		var personlabel string
		switch access.Type {
		default:
			// only care about read and edit permission levels
			continue
		case "read":
			grouplabel = "read-group"
			personlabel = "read-person"
		case "edit":
			grouplabel = "edit-group"
			personlabel = "edit-person"
		}
		for _, g := range access.Groups {
			result.Add(grouplabel, g)
		}
		for _, p := range access.Persons {
			result.Add(personlabel, p)
		}
	}

	return nil
}

type propertiesDS struct {
	Depositor      string `xml:"depositor"`
	Owner          string `xml:"owner"`
	Representative string `xml:"representative"`
}

func ReadProperties(remote *remoteFedora, id string, result *CurateItem) error {
	body, err := remote.GetDatastream(id, "properties")
	if err != nil {
		return err
	}
	var props propertiesDS
	dec := xml.NewDecoder(body)
	err = dec.Decode(&props)
	body.Close()
	if err != nil {
		return err
	}

	result.Add("depositor", props.Depositor)
	result.Add("owner", props.Owner)
	result.Add("representative", props.Representative)

	return nil
}

func ReadContent(remote *remoteFedora, id string, result *CurateItem) error {
	info, err := remote.GetDatastreamInfo(id, "content")
	if err != nil {
		return err
	}

	result.Add("filename", info.Label)
	result.Add("checksum-md5", info.Checksum)
	result.Add("mime-type", info.MIMEType)
	//result.Add("file-size", info.Size) // convert to string
	result.Add("file-location", info.Location)

	return nil
}

func ReadThumbnail(remote *remoteFedora, id string, result *CurateItem) error {
	info, err := remote.GetDatastreamInfo(id, "thumbnail")
	if err != nil {
		return err
	}

	result.Add("thumbnail", info.Location)

	return nil
}

func ReadBendoItem(remote *remoteFedora, id string, result *CurateItem) error {
	body, err := remote.GetDatastream(id, "bendo-item")
	if err != nil {
		return err
	}
	v, err := ioutil.ReadAll(body)
	body.Close()
	if err != nil {
		return err
	}

	result.Add("bendo-item", string(v))

	return nil
}
