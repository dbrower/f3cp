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
	remote := NewRemote(os.Args[2])
	switch os.Args[1] {
	case "dump":
		DumpList(remote, os.Stdout, os.Args[3:])
	case "load":
		LoadList(remote, os.Stdin)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command %s", os.Args[1])
	}
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
