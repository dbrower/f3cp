package main

import (
	"bytes"
	"encoding/json"
	//	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"unicode/utf8"
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
		return
	}
	rf := NewRemote(os.Args[2])
	switch os.Args[1] {
	case "dump":
		DumpList(rf, os.Stdout, os.Args[3:])
	case "load":
		LoadList(rf, os.Stdin)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command %s", os.Args[1])
	}
}

// DumpList dumps a list of ids from the given fedora instance to out as
// an well-formed JSON array. The objects are output in the order given
// in the id array. Only one object is kept in memory at a time, so this
// can handle a long list of objects. Status updates and errors are printed to
// STDERR.
func DumpList(rf *remoteFedora, out io.Writer, ids []string) error {
	enc := json.NewEncoder(out)
	fmt.Fprintf(out, "[")
	first := true
	for _, id := range ids {
		if !first {
			fmt.Fprintf(out, ",")
		}
		first = false
		fmt.Fprintln(os.Stderr, "dumping", id)
		obj, err := FetchOneObject(rf, id)
		if err != nil {
			fmt.Fprintf(os.Stderr, id, err)
			continue
		}
		enc.Encode(obj)
	}
	fmt.Fprintf(out, "]")
	return nil
}

// FetchOneObject loads every datastream from the fedora object id from rf and
// returns it as an FObject. It loads all of the datastreams in memory, so
// there is a potential for extremely large objects to run out of memory.
func FetchOneObject(rf *remoteFedora, id string) (*FObject, error) {
	obj, err := rf.GetObjectInfo(id)
	if err != nil {
		return nil, err
	}

	result := FObject{ObjectInfo: obj}
	dsNames, err := rf.GetDatastreamList(id)
	if err != nil {
		return nil, err
	}
	// load the datastreams in alphabetical order
	sort.StringSlice(dsNames).Sort()
	for _, ds := range dsNames {
		var entry DSentry
		entry.DsInfo, err = rf.GetDatastreamInfo(id, ds)
		if err != nil {
			return nil, err
		}
		if entry.Size > 0 {
			body, err := rf.GetDatastream(id, ds)
			if err != nil {
				return nil, err
			}
			entry.ContentBase64, err = ioutil.ReadAll(body)
			if err != nil {
				return nil, err
			}
			body.Close()
			if utf8.Valid(entry.ContentBase64) {
				entry.Content = string(entry.ContentBase64)
				entry.ContentBase64 = nil
			}
		}
		result.DSitems = append(result.DSitems, entry)
	}
	return &result, nil
}

func LoadList(rf *remoteFedora, source io.Reader) error {
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
		err = UploadOneObject(rf, obj)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return err
		}
	}

	// read closing bracket
	_, err = dec.Token()
	return err
}

func UploadOneObject(rf *remoteFedora, obj FObject) error {
	// does object exist?
	_, err := rf.GetObjectInfo(obj.PID)
	if err == ErrNotFound {
		err = rf.MakeObject(obj.ObjectInfo)
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
		var source io.Reader
		if ds.Content != "" {
			source = strings.NewReader(ds.Content)
		} else if len(ds.ContentBase64) > 0 {
			source = bytes.NewReader(ds.ContentBase64)
		}
		_, err = rf.GetDatastreamInfo(obj.PID, ds.Name)
		if err == ErrNotFound {
			err = rf.MakeDatastream(obj.PID, ds.DsInfo, source)
		} else if err == nil {
			err = rf.UpdateDatastream(obj.PID, ds.DsInfo, source)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
