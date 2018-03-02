package main

import (
	"bytes"
	"encoding/json"
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
		fmt.Fprintf(os.Stderr, "Unknown verb %s", os.Args[1])
	}
}

func DumpList(rf *remoteFedora, out io.Writer, ids []string) error {
	fmt.Fprintf(out, "[")
	first := true
	for _, id := range ids {
		if !first {
			fmt.Fprintf(out, ",")
		}
		first = false
		fmt.Fprintln(os.Stderr, "dumping", id)
		err := DumpOneObject(rf, out, id)
		if err != nil {
			fmt.Fprintf(os.Stderr, id, err)
			continue
		}
	}
	fmt.Fprintf(out, "]")
	return nil
}

func DumpOneObject(rf *remoteFedora, out io.Writer, id string) error {
	obj, err := rf.GetObjectInfo(id)
	if err != nil {
		return err
	}

	result := FObject{ObjectInfo: obj}
	dsNames, err := rf.GetDatastreamList(id)
	if err != nil {
		return err
	}
	sort.StringSlice(dsNames).Sort()
	for _, ds := range dsNames {
		var entry DSentry
		entry.DsInfo, err = rf.GetDatastreamInfo(id, ds)
		if err != nil {
			return err
		}
		if entry.Size > 0 {
			body, err := rf.GetDatastream(id, ds)
			if err != nil {
				return err
			}
			entry.ContentBase64, err = ioutil.ReadAll(body)
			if err != nil {
				return err
			}
			body.Close()
			if utf8.Valid(entry.ContentBase64) {
				entry.Content = string(entry.ContentBase64)
				entry.ContentBase64 = nil
			}
		}
		result.DSitems = append(result.DSitems, entry)
	}
	enc := json.NewEncoder(out)
	enc.Encode(result)
	return nil
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
		var source io.Reader
		if ds.Content != "" {
			source = strings.NewReader(ds.Content)
		} else if len(ds.ContentBase64) > 0 {
			source = bytes.NewReader(ds.ContentBase64)
		} else {
			source = strings.NewReader("")
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
